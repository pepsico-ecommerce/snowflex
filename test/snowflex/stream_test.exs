defmodule Snowflex.StreamTest do
  # Drives the DBConnection cursor path (handle_declare → handle_fetch →
  # handle_deallocate) end-to-end against a scripted transport.
  #
  # Regression: Snowflex.Connection.handle_fetch/4 used to match only
  # {:cont, result} from the transport, but transports return {:ok, result}
  # per the Snowflex.Transport contract, so the first fetch of every cursor
  # crashed with a CaseClauseError. The {:cont, ...} branch also returned a
  # {:cont, result, query, state} 4-tuple, which DBConnection rejects.
  use ExUnit.Case, async: true

  alias Snowflex.Query
  alias Snowflex.Result

  defmodule PartitionedTransport do
    # Serves a scripted list of partitions: declare returns the max partition
    # index (mirroring Snowflex.Transport.Http), each fetch pops the next
    # partition, and an exhausted cursor halts with an empty result.
    @behaviour Snowflex.Transport

    use GenServer

    @impl Snowflex.Transport
    def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

    @impl GenServer
    def init(opts), do: {:ok, %{partitions: Keyword.fetch!(opts, :partitions), next: 0}}

    @impl Snowflex.Transport
    def execute_statement(pid, _statement, _params, _opts), do: GenServer.call(pid, :execute)

    @impl Snowflex.Transport
    def declare(pid, _statement, _params, _opts), do: GenServer.call(pid, :declare)

    @impl Snowflex.Transport
    def fetch(pid, _cursor, _opts), do: GenServer.call(pid, :fetch)

    @impl Snowflex.Transport
    def deallocate(pid), do: GenServer.call(pid, :deallocate)

    @impl Snowflex.Transport
    def disconnect(pid) do
      if Process.alive?(pid), do: Process.exit(pid, :normal)
      :ok
    end

    @impl Snowflex.Transport
    def ping(_pid), do: {:ok, %Result{}}

    @impl GenServer
    def handle_call(:execute, _from, state), do: {:reply, {:ok, %Result{}}, state}

    # Deliberately does NOT reset :next — only deallocate does — so tests can
    # assert that DBConnection deallocates the cursor between streams.
    def handle_call(:declare, _from, %{partitions: partitions} = state) do
      {:reply, {:ok, length(partitions) - 1}, state}
    end

    def handle_call(:fetch, _from, %{partitions: partitions, next: next} = state) do
      case Enum.at(partitions, next) do
        nil ->
          {:reply, {:halt, %Result{}}, state}

        {:error, _reason} = error ->
          {:reply, error, state}

        rows ->
          result = %Result{columns: ["n"], rows: rows, num_rows: length(rows)}
          {:reply, {:ok, result}, %{state | next: next + 1}}
      end
    end

    def handle_call(:deallocate, _from, state), do: {:reply, :ok, %{state | next: 0}}
  end

  defp start_pool!(partitions) do
    {:ok, pool} =
      DBConnection.start_link(Snowflex.Connection,
        transport: PartitionedTransport,
        partitions: partitions,
        pool_size: 1,
        idle_interval: 60_000
      )

    pool
  end

  defp stream_all(pool) do
    DBConnection.run(pool, fn conn ->
      conn
      |> DBConnection.prepare_stream(%Query{statement: "SELECT n FROM t"}, [], [])
      |> Enum.to_list()
    end)
  end

  test "emits one result per partition, then the exhausted halt result" do
    pool = start_pool!([[["1"], ["2"]], [["3"]]])

    assert [
             %Result{rows: [["1"], ["2"]], num_rows: 2},
             %Result{rows: [["3"]], num_rows: 1},
             %Result{rows: nil, num_rows: 0}
           ] = stream_all(pool)
  end

  test "streams a single-partition result" do
    pool = start_pool!([[["only"]]])

    assert [%Result{rows: [["only"]]}, %Result{rows: nil}] = stream_all(pool)
  end

  test "a fetch error surfaces as a raised Snowflex.Error" do
    pool = start_pool!([[["1"]], {:error, %Snowflex.Error{message: "partition kaput"}}])

    assert_raise Snowflex.Error, "partition kaput", fn ->
      stream_all(pool)
    end
  end

  test "the cursor is deallocated after enumeration so the connection can stream again" do
    pool = start_pool!([[["1"]], [["2"]]])

    assert [%Result{rows: [["1"]]}, %Result{rows: [["2"]]}, %Result{rows: nil}] =
             stream_all(pool)

    # deallocate reset the scripted transport, so a second stream on the same
    # pooled connection starts from the first partition again
    assert [%Result{rows: [["1"]]}, %Result{rows: [["2"]]}, %Result{rows: nil}] =
             stream_all(pool)
  end
end
