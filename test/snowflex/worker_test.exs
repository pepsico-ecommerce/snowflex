defmodule Snowflex.WorkerTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  @connection_args [
    server: "snowflex.us-east-8.snowflakecomputing.com",
    role: "DEV",
    warehouse: "CUSTOMER_DEV_WH"
  ]

  setup do
    :meck.new(:odbc, [:no_link])
    on_exit(fn -> :meck.unload(:odbc) end)
  end

  describe "keep alive" do
    setup do
      :meck.expect(:odbc, :connect, fn _, _ -> {:ok, "mock pid"} end)
      :meck.expect(:odbc, :sql_query, fn "mock pid", 'SELECT 1' -> "1" end)
      on_exit(fn -> assert :meck.validate(:odbc) end)
    end

    test "does not send a heartbeat if `keep_alive?` is false" do
      start_supervised!({Snowflex.Worker, {@connection_args, false, 10}})
      Process.sleep(15)

      assert :meck.num_calls(:odbc, :sql_query, ["mock pid", 'SELECT 1']) == 0
    end

    test "sends heartbeat every interval if `keep_alive?` is true" do
      assert capture_log(fn ->
               start_supervised!({Snowflex.Worker, {@connection_args, true, 10}})
               Process.sleep(30)
             end) =~ "sending heartbeat"

      assert :meck.num_calls(:odbc, :sql_query, ["mock pid", 'SELECT 1']) > 1
    end

    test "postpones heartbeat if any other sql query is sent" do
      :meck.expect(:odbc, :sql_query, fn "mock pid", 'SELECT * FROM my_table' ->
        {:selected, ['name'], [{'dustin'}]}
      end)

      refute(
        capture_log(fn ->
          worker = start_supervised!({Snowflex.Worker, {@connection_args, true, 10}})
          Process.sleep(7)
          Snowflex.Worker.sql_query(worker, "SELECT * FROM my_table")
          Process.sleep(7)
        end) =~ "sending heartbeat"
      )

      assert :meck.num_calls(:odbc, :sql_query, ["mock pid", 'SELECT 1']) == 0
    end

    test "postpones heartbeat if any other params query is sent" do
      :meck.expect(:odbc, :param_query, fn "mock pid",
                                           'SELECT * FROM my_table WHERE name=?',
                                           [{{:sql_varchar, 250}, ['dustin']}] ->
        {:selected, ['name'], [{'dustin'}]}
      end)

      refute(
        capture_log(fn ->
          worker = start_supervised!({Snowflex.Worker, {@connection_args, true, 10}})
          Process.sleep(7)

          Snowflex.Worker.param_query(worker, "SELECT * FROM my_table WHERE name=?", [
            Snowflex.string_param("dustin")
          ])

          Process.sleep(7)
        end) =~ "sending heartbeat"
      )

      assert :meck.num_calls(:odbc, :sql_query, ["mock pid", 'SELECT 1']) == 0
    end
  end
end
