defmodule Snowflex.ConnectionTest do
  use ExUnit.Case, async: true

  defmodule SnowflakeConnection do
    use Snowflex.Connection,
      otp_app: :snowflex
  end

  defmodule MockWorker do
    use GenServer

    # API

    def start_link(_) do
      GenServer.start_link(__MODULE__, nil, [])
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
      {:reply, {:ok, {:updated, 123}}, state}
    end

    def handle_call({:param_query, _, _}, _from, state) do
      {:reply, {:ok, {:selected, ['col'], [{1}, {2}]}}, state}
    end
  end

  describe "execute/1" do
    test "should execute a sql query" do
      start_supervised!(SnowflakeConnection)

      assert [%{"col" => 1}, %{"col" => 2}] == SnowflakeConnection.execute("my query")
    end

    test "should execute an insert query" do
      start_supervised!(SnowflakeConnection)

      assert {:updated, 123} == SnowflakeConnection.execute("insert query")
    end

    test "should execute a sql query with utf-16 artifacts and scrub it" do
      start_supervised!(SnowflakeConnection)
      # above we set this to return 'CVS PharmacyÂ®', as we saw in the real world, and then
      # here we ensure that we just have the ® character
      assert [%{"col" => "CVS Pharmacy®"}] == SnowflakeConnection.execute("return_utf_16")
    end
  end

  describe "execute/2" do
    test "should execute a param query" do
      start_supervised!(SnowflakeConnection)

      assert [%{"col" => 1}, %{"col" => 2}] == SnowflakeConnection.execute("my query", [])
    end

    test "should execute an insert param query" do
      start_supervised!(SnowflakeConnection)

      assert {:updated, 123} == SnowflakeConnection.execute("insert query", [])
    end
  end
end
