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

    def sql_query(pid, query, _opts) do
      GenServer.call(pid, {:sql_query, query}, 1000)
    end

    def param_query(pid, query, params, _opts) do
      GenServer.call(pid, {:param_query, query, params}, 1000)
    end

    # Callbacks

    def init(_) do
      {:ok, %{}}
    end

    def handle_call({:sql_query, "return_utf_16"}, _from, state) do
      {:reply, {:ok, {:selected, ['col'], [{'CVS PharmacyÂ®'}]}}, state}
    end

    def handle_call({:sql_query, "insert " <> _}, _from, state) do
      {:reply, {:ok, {:updated, 123}}, state}
    end

    def handle_call({:sql_query, _}, _from, state) do
      {:reply, {:ok, {:selected, ['col'], [{1}, {2}]}}, state}
    end

    def handle_call({:param_query, "insert " <> _, _}, _from, state) do
      {:reply, {:ok, {:updated, 456}}, state}
    end

    def handle_call({:param_query, _, _}, _from, state) do
      {:reply, {:ok, {:selected, ['col'], [{1}, {2}]}}, state}
    end
  end

  setup_all do
    start_supervised!(SnowflakeDBConnection)

    :ok
  end

  describe "execute/1" do
    test "should execute a sql query" do
      assert {:ok, result} = SnowflakeDBConnection.execute("my query")
      assert [%{"col" => 1}, %{"col" => 2}] == SnowflakeDBConnection.process_result(result)
    end

    test "should execute an insert query" do
      assert {:ok, result} = SnowflakeDBConnection.execute("insert query")
      assert {:updated, 123} == SnowflakeDBConnection.process_result(result)
    end

    test "should execute a sql query with utf-16 artifacts and scrub it" do
      assert {:ok, %{rows: [{charlist}]}} = SnowflakeDBConnection.execute("return_utf_16")
      # above we set this to return 'CVS PharmacyÂ®', as we saw in the real world, and then
      # here we ensure that we just have the ® character
      string = Enum.into(charlist, <<>>, fn bit -> <<bit>> end)
      assert string == "CVS Pharmacy®"
    end
  end

  describe "execute/2" do
    test "should execute a param query" do
      assert {:ok, result} = SnowflakeDBConnection.execute("my query", [])
      assert [%{"col" => 1}, %{"col" => 2}] == SnowflakeDBConnection.process_result(result)
    end

    test "should execute an insert param query" do
      assert {:ok, result} = SnowflakeDBConnection.execute("insert query", ["params"])
      assert {:updated, 456} == SnowflakeDBConnection.process_result(result)
    end
  end
end
