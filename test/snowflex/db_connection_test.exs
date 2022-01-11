defmodule Snowflex.DBConnectionTest do
  use ExUnit.Case, async: true

  defmodule SnowflakeDBConnection do
    use Snowflex.DBConnection,
      otp_app: :snowflex
  end

  defmodule MockWorker do
    use GenServer

    # API

    def start_link(_) do
      GenServer.start_link(__MODULE__, nil, [])
    end

    def sql_query(pid, statement, _opts) do
      GenServer.call(pid, {:sql_query, %{statement: statement}}, 1000)
    end

    def param_query(pid, statement, params, _opts) do
      GenServer.call(pid, {:param_query, %{statement: statement, params: params}}, 1000)
    end

    # Callbacks

    def init(_) do
      {:ok, %{}}
    end

    def handle_call({:sql_query, "insert " <> _}, _from, state) do
      {:reply, {:ok, {:updated, 123}}, state}
    end

    def handle_call({:sql_query, _}, _from, state) do
      {:reply, {:ok, {:selected, ['col'], [{1}, {2}]}}, state}
    end

    def handle_call({:param_query, "insert " <> _, _}, _from, state) do
      {:reply, {:ok, {:updated, 123}}, state}
    end

    def handle_call({:param_query, _, _}, _from, state) do
      {:reply, {:ok, {:selected, ['col'], [{1}, {2}]}}, state}
    end
  end

  describe "execute/1" do
    test "should execute a sql query" do
      start_supervised!(SnowflakeDBConnection)

      assert {:ok, _result} = SnowflakeDBConnection.execute("my query")
    end

    test "should execute an insert query" do
      start_supervised!(SnowflakeDBConnection)

      assert {:updated, 123} == SnowflakeDBConnection.execute("insert query")
    end
  end

  describe "execute/2" do
    test "should execute a param query" do
      start_supervised!(SnowflakeDBConnection)

      assert [%{"col" => 1}, %{"col" => 2}] == SnowflakeDBConnection.execute("my query", [])
    end

    test "should execute an insert param query" do
      start_supervised!(SnowflakeDBConnection)

      assert {:updated, 123} == SnowflakeDBConnection.execute("insert query", [])
    end
  end
end
