defmodule Snowflex.Connection.RecycleTest do
  # End-to-end coverage of the bug fixed in PR #176: when the underlying
  # transport process dies, the DBConnection pool worker must be recycled so
  # subsequent queries succeed instead of failing forever with :noproc.
  #
  # Snowflex.Connection.connect/1 starts the transport via start_link/1 and
  # does NOT trap exits, so the transport's death propagates through the link
  # and brings the worker down with it. The pool detects the dead worker and
  # starts a replacement, which calls connect/1 → start_link → fresh transport.
  use ExUnit.Case, async: false

  alias Snowflex.Error
  alias Snowflex.Query
  alias Snowflex.Result

  defmodule TattlingTransport do
    @moduledoc """
    Transport that messages the test process every time start_link runs,
    reporting both its own pid and the pid of the DBConnection worker that
    spawned it (the only thing it's linked to at that point).
    """
    @behaviour Snowflex.Transport

    use GenServer

    @impl Snowflex.Transport
    def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

    @impl GenServer
    def init(opts) do
      watcher = Keyword.fetch!(opts, :watcher)
      {:links, [worker_pid]} = Process.info(self(), :links)
      send(watcher, {:transport_started, self(), worker_pid})
      {:ok, %{}}
    end

    @impl Snowflex.Transport
    def execute_statement(pid, _statement, _params, _opts) do
      GenServer.call(pid, :execute)
    catch
      :exit, reason -> {:error, %Error{message: "execute failed: #{inspect(reason)}"}}
    end

    @impl Snowflex.Transport
    def declare(_pid, _statement, _params, _opts), do: {:ok, 0}

    @impl Snowflex.Transport
    def fetch(_pid, _cursor, _opts), do: {:halt, %Result{}}

    @impl Snowflex.Transport
    def disconnect(pid) do
      if Process.alive?(pid), do: Process.exit(pid, :normal)
      :ok
    end

    @impl Snowflex.Transport
    def deallocate(_pid), do: :ok

    @impl Snowflex.Transport
    def ping(_pid), do: {:ok, %Result{}}

    @impl GenServer
    def handle_call(:execute, _from, state),
      do: {:reply, {:ok, %Result{columns: ["1"], rows: [[1]]}}, state}
  end

  defp start_pool!(test_pid) do
    {:ok, pool} =
      DBConnection.start_link(Snowflex.Connection,
        transport: TattlingTransport,
        watcher: test_pid,
        pool_size: 1,
        backoff_min: 0,
        backoff_max: 0,
        idle_interval: 60_000
      )

    pool
  end

  defp execute(pool) do
    DBConnection.execute(pool, %Query{statement: "SELECT 1"}, [], [])
  end

  defp execute_until_ok(pool, attempts \\ 100) do
    Enum.reduce_while(1..attempts, nil, fn _, _ ->
      case execute(pool) do
        {:ok, _, %Result{}} = ok -> {:halt, ok}
        _ -> Process.sleep(20) && {:cont, nil}
      end
    end)
  end

  test "killing the transport recycles the worker and queries recover" do
    test_pid = self()
    pool = start_pool!(test_pid)

    assert_receive {:transport_started, transport_pid, worker_pid}, 1_000
    refute_received {:transport_started, _, _}

    {:ok, _, %Result{rows: [[1]]}} = execute(pool)

    transport_ref = Process.monitor(transport_pid)
    worker_ref = Process.monitor(worker_pid)

    Process.exit(transport_pid, :kill)

    # The link from worker → transport is what propagates death. Without
    # trapping exits, the worker dies with the same reason.
    assert_receive {:DOWN, ^transport_ref, :process, ^transport_pid, _}, 1_000
    assert_receive {:DOWN, ^worker_ref, :process, ^worker_pid, _}, 1_000

    # The pool restarts the worker, which calls connect/1 → start_link →
    # a fresh transport tatts on init. Both pids must be different.
    assert_receive {:transport_started, new_transport_pid, new_worker_pid}, 2_000
    refute new_transport_pid == transport_pid
    refute new_worker_pid == worker_pid

    assert {:ok, _, %Result{rows: [[1]]}} = execute_until_ok(pool)
  end

  test "queries continue to work after multiple successive transport crashes" do
    test_pid = self()
    pool = start_pool!(test_pid)

    assert_receive {:transport_started, transport_pid, _worker}, 1_000

    transport_pid =
      Enum.reduce(1..3, transport_pid, fn _, current_transport ->
        assert {:ok, _, %Result{rows: [[1]]}} = execute_until_ok(pool)

        ref = Process.monitor(current_transport)
        Process.exit(current_transport, :kill)
        assert_receive {:DOWN, ^ref, :process, ^current_transport, _}, 1_000

        assert_receive {:transport_started, new_pid, _new_worker}, 2_000
        refute new_pid == current_transport
        new_pid
      end)

    assert {:ok, _, %Result{rows: [[1]]}} = execute_until_ok(pool)
    assert Process.alive?(transport_pid)
  end
end
