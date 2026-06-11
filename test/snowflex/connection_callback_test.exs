defmodule Snowflex.Connection.CallbackTest do
  use ExUnit.Case, async: true

  alias Snowflex.Connection
  alias Snowflex.Error
  alias Snowflex.Result

  defmodule StubTransport do
    @moduledoc false
    @behaviour Snowflex.Transport

    use GenServer

    @impl Snowflex.Transport
    def start_link(opts) do
      GenServer.start_link(__MODULE__, opts)
    end

    @impl GenServer
    def init(_opts), do: {:ok, %{}}

    @impl Snowflex.Transport
    def execute_statement(pid, _statement, _params, _opts) do
      GenServer.call(pid, :execute)
    catch
      :exit, reason ->
        {:error, %Error{message: "execute failed due to #{inspect(reason)}"}}
    end

    @impl Snowflex.Transport
    def declare(pid, _statement, _params, _opts) do
      GenServer.call(pid, :declare)
    catch
      :exit, reason ->
        {:error, %Error{message: "declare failed due to #{inspect(reason)}"}}
    end

    @impl Snowflex.Transport
    def fetch(pid, _cursor, _opts) do
      GenServer.call(pid, :fetch)
    catch
      :exit, reason ->
        {:error, %Error{message: "fetch failed due to #{inspect(reason)}"}}
    end

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
    def handle_call(:execute, _from, state), do: {:reply, {:ok, %Result{}}, state}
    def handle_call(:declare, _from, state), do: {:reply, {:ok, 0}, state}
    def handle_call(:fetch, _from, state), do: {:reply, {:halt, %Result{}}, state}
  end

  defp build_state(transport_pid) do
    %Connection{
      pid: transport_pid,
      transport: StubTransport,
      state: :connected,
      opts: []
    }
  end

  defp query, do: %{statement: "SELECT 1"}

  describe "handle_execute/4 with dead transport" do
    test "returns :disconnect when transport pid is dead before the call" do
      {:ok, transport} = StubTransport.start_link([])
      Process.unlink(transport)
      ref = Process.monitor(transport)
      Process.exit(transport, :kill)
      assert_receive {:DOWN, ^ref, :process, ^transport, _}, 1_000
      refute Process.alive?(transport)

      state = build_state(transport)

      assert {:disconnect, %Error{}, ^state} =
               Connection.handle_execute(query(), [], [], state)
    end

    test "returns :disconnect when transport dies mid-call" do
      {:ok, transport} = StubTransport.start_link([])
      Process.unlink(transport)
      Process.exit(transport, :kill)

      # Loop until alive? returns false to avoid relying on scheduling.
      Enum.each(1..200, fn _ ->
        if Process.alive?(transport), do: Process.sleep(5)
      end)

      refute Process.alive?(transport)

      state = build_state(transport)

      assert {:disconnect, %Error{}, ^state} =
               Connection.handle_execute(query(), [], [], state)
    end

    test "returns :error when transport is alive and returns an application error" do
      defmodule AppErrorTransport do
        @moduledoc false
        @behaviour Snowflex.Transport

        use GenServer

        @impl Snowflex.Transport
        def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

        @impl GenServer
        def init(_), do: {:ok, %{}}

        @impl Snowflex.Transport
        def execute_statement(_pid, _s, _p, _o),
          do: {:error, %Error{message: "syntax error", code: "001003"}}

        @impl Snowflex.Transport
        def declare(_, _, _, _), do: {:ok, 0}
        @impl Snowflex.Transport
        def fetch(_, _, _), do: {:halt, %Result{}}
        @impl Snowflex.Transport
        def disconnect(_), do: :ok
        @impl Snowflex.Transport
        def deallocate(_), do: :ok
        @impl Snowflex.Transport
        def ping(_), do: {:ok, %Result{}}
      end

      {:ok, transport} = AppErrorTransport.start_link([])

      state = %Connection{
        pid: transport,
        transport: AppErrorTransport,
        state: :connected,
        opts: []
      }

      assert {:error, %Error{message: "syntax error"}, ^state} =
               Connection.handle_execute(query(), [], [], state)

      assert Process.alive?(transport)
    end
  end

  describe "handle_declare/4 with dead transport" do
    test "returns :disconnect when transport pid is dead" do
      {:ok, transport} = StubTransport.start_link([])
      Process.unlink(transport)
      Process.exit(transport, :kill)

      Enum.each(1..50, fn _ ->
        if Process.alive?(transport), do: Process.sleep(5)
      end)

      state = build_state(transport)

      assert {:disconnect, %Error{}, ^state} =
               Connection.handle_declare(query(), [], [], state)
    end
  end

  describe "handle_fetch/4 with dead transport" do
    test "returns :disconnect when transport pid is dead" do
      {:ok, transport} = StubTransport.start_link([])
      Process.unlink(transport)
      Process.exit(transport, :kill)

      Enum.each(1..50, fn _ ->
        if Process.alive?(transport), do: Process.sleep(5)
      end)

      state = build_state(transport)

      assert {:disconnect, %Error{}, ^state} =
               Connection.handle_fetch(query(), 0, [], state)
    end
  end

  describe "handle_deallocate/4 with dead transport" do
    test "is a no-op when transport pid is dead (cast cannot detect)" do
      {:ok, transport} = StubTransport.start_link([])
      Process.unlink(transport)
      Process.exit(transport, :kill)

      Enum.each(1..50, fn _ ->
        if Process.alive?(transport), do: Process.sleep(5)
      end)

      state = build_state(transport)

      # deallocate is a GenServer.cast — it cannot observe a dead pid, so it
      # silently succeeds. The next handle_execute will trigger the recycle.
      assert {:ok, ^transport, ^state} =
               Connection.handle_deallocate(query(), 0, [], state)
    end
  end
end
