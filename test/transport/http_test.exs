defmodule Snowflex.Transport.HttpTest do
  use ExUnit.Case

  alias Snowflex.Error
  alias Snowflex.Transport.Http

  defmodule DummyHttp do
    use GenServer

    def start_link(_) do
      GenServer.start_link(__MODULE__, %{})
    end

    @impl GenServer
    def init(state) do
      {:ok, state}
    end

    @impl GenServer
    def handle_call({:execute, _statement, _params, _opts}, _from, state) do
      Process.sleep(50)
      {:reply, state, state}
    end
  end

  test "execute_statement/4 handles timeout" do
    {:ok, pid} = start_supervised(DummyHttp, %{})

    assert {:error, %Error{message: "Select 1 timed out after 10"}} =
             Http.execute_statement(pid, "Select 1", nil, timeout: 10)

    assert %{} = Http.execute_statement(pid, nil, nil, timeout: 1000)
  end
end
