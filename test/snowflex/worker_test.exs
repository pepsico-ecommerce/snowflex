defmodule Snowflex.WorkerTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Snowflex.Worker

  @connection_args [
    server: "snowflex.us-east-8.snowflakecomputing.com",
    role: "DEV",
    warehouse: "CUSTOMER_DEV_WH"
  ]
  @without_keep_alive [
    connection_args: @connection_args,
    keep_alive?: false,
    heartbeat_interval: 10
  ]
  @with_keep_alive [
    connection_args: @connection_args,
    keep_alive?: true,
    heartbeat_interval: 10
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
      start_supervised!({Snowflex.Worker, @without_keep_alive})
      Process.sleep(15)

      assert :meck.num_calls(:odbc, :sql_query, ["mock pid", 'SELECT 1']) == 0
    end

    test "sends heartbeat every interval if `keep_alive?` is true" do
      assert capture_log(fn ->
               start_supervised!({Snowflex.Worker, @with_keep_alive})
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
          worker = start_supervised!({Snowflex.Worker, @with_keep_alive})
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
          worker = start_supervised!({Snowflex.Worker, @with_keep_alive})
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

      attach(start_req_id, [:snowflex, :sql_query, :start], self())
      attach(stop_req_id, [:snowflex, :sql_query, :stop], self())

      :meck.expect(:odbc, :sql_query, fn "mock pid", 'SELECT * FROM my_table' ->
        {:selected, ['name'], [{'dustin'}]}
      end)

      worker = start_supervised!({Snowflex.Worker, @with_keep_alive})
      Snowflex.Worker.sql_query(worker, "SELECT * FROM my_table")

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
        worker = start_supervised!({Worker, @with_keep_alive})
        Worker.param_query(worker, query, params)
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
        worker = start_supervised!({Worker, @with_keep_alive})
        Worker.param_query(worker, query, params)
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
