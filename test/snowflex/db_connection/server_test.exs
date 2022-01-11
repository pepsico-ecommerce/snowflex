defmodule Snowflex.DBConnection.ServerTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Snowflex.DBConnection.Server

  @connection_args [
    connection: [
      server: "snowflex.us-east-8.snowflakecomputing.com",
      role: "DEV",
      warehouse: "CUSTOMER_DEV_WH"
    ]
  ]

  setup do
    :meck.new(:odbc, [:no_link])
    on_exit(fn -> :meck.unload(:odbc) end)
  end

  describe "telemetry events" do
    setup do
      :meck.expect(:odbc, :connect, fn _, _ -> {:ok, "mock pid"} end)
      :meck.expect(:odbc, :sql_query, fn "mock pid", 'SELECT 1' -> "1" end)
      on_exit(fn -> assert :meck.validate(:odbc) end)
    end

    test "sends start and stop events" do
      start_req_id = {:start, :rand.uniform(100)}
      stop_req_id = {:stop, :rand.uniform(100)}

      on_exit(fn ->
        :telemetry.detach(start_req_id)
        :telemetry.detach(stop_req_id)
      end)

      capture_log(fn ->
        attach(start_req_id, [:snowflex, :sql_query, :start], self())
        attach(stop_req_id, [:snowflex, :sql_query, :stop], self())
      end)

      :meck.expect(:odbc, :sql_query, fn "mock pid", 'SELECT * FROM my_table' ->
        {:selected, ['name'], [{'dustin'}]}
      end)

      capture_log(fn ->
        server = start_supervised!({Server, @connection_args})
        Server.sql_query(server, "SELECT * FROM my_table")
      end)

      assert_received {:event, [:snowflex, :sql_query, :start], %{system_time: _},
                       %{query: "SELECT * FROM my_table"}}

      assert_received {:event, [:snowflex, :sql_query, :stop], %{duration: _}, %{}}
    end
  end

  describe "param_query" do
    setup do
      :meck.expect(:odbc, :connect, fn _, _ -> {:ok, "mock pid"} end)
      on_exit(fn -> assert :meck.validate(:odbc) end)
    end

    test "with a string type, converts nil values to :null and strings to charlists" do
      :meck.expect(:odbc, :param_query, fn "mock pid",
                                           '[some param query]',
                                           [{{:sql_varchar, 255}, ['abc', :null, 'def']}] ->
        "1"
      end)

      query = "[some param query]"
      params = [{{:sql_varchar, 255}, ["abc", nil, "def"]}]

      capture_log(fn ->
        server = start_supervised!({Server, @connection_args})
        Server.param_query(server, query, params)
      end)
    end

    test "with an integer type, converts nil values to :null" do
      :meck.expect(:odbc, :param_query, fn "mock pid",
                                           '[some param query]',
                                           [{{:sql_integer, 255}, [123, :null, 456]}] ->
        "1"
      end)

      query = "[some param query]"
      params = [{{:sql_integer, 255}, [123, nil, 456]}]

      capture_log(fn ->
        server = start_supervised!({Server, @connection_args})
        Server.param_query(server, query, params)
      end)
    end
  end

  defp attach(handler_id, event, pid) do
    :telemetry.attach(
      handler_id,
      event,
      fn event, measurements, metadata, _ ->
        send(pid, {:event, event, measurements, metadata})
      end,
      nil
    )
  end
end
