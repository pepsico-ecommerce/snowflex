defmodule Snowflex.AsyncStatementTest do
  use ExUnit.Case, async: false

  import Req.Test, only: [set_req_test_to_shared: 1]

  alias Plug.Conn
  alias Req.Test, as: ReqTest

  @handle "01b7e043-0206-7a43-0008-8b8300073d86"

  setup :set_req_test_to_shared

  defmodule AsyncRepo do
    use Ecto.Repo,
      otp_app: :snowflex,
      adapter: Snowflex
  end

  defp private_key_path, do: Path.join(File.cwd!(), "test/fixtures/fake_private_key.pem")

  defp start_repo(opts \\ []) do
    start_link_supervised!(
      {AsyncRepo,
       Keyword.merge(
         [
           account_name: "test_acc",
           username: "test_usr",
           private_key_path: private_key_path(),
           req_options: [plug: {Req.Test, MockAsyncHttp}]
         ],
         opts
       )}
    )
  end

  defp json(conn, status, body) do
    conn
    |> Conn.put_resp_content_type("application/json")
    |> Conn.send_resp(status, Jason.encode!(body))
  end

  # The boot-time health check issues "SELECT 1" before any test traffic.
  defp health_check?(%{params: %{"statement" => "SELECT 1"}}), do: true
  defp health_check?(_conn), do: false

  describe "submit_async/4" do
    test "sends async=true and returns the handle without polling for completion" do
      test_pid = self()

      ReqTest.stub(MockAsyncHttp, fn conn ->
        cond do
          health_check?(conn) ->
            ReqTest.json(conn, %{})

          conn.method == "POST" ->
            send(test_pid, {:submitted, conn.params, conn.query_string})
            json(conn, 202, %{"statementHandle" => @handle})

          true ->
            # A GET here means the transport polled for the result, which is
            # exactly what fire-and-forget must not do.
            send(test_pid, {:polled, conn.request_path})
            json(conn, 200, %{"statementHandle" => @handle})
        end
      end)

      start_repo()

      assert {:ok, @handle} = Snowflex.submit_async(AsyncRepo, "CALL long_proc()")

      assert_received {:submitted, params, query_string}
      assert params["statement"] == "CALL long_proc()"
      assert query_string =~ "async=true"
      refute_received {:polled, _path}
    end

    test "passes bindings and query_tag through" do
      test_pid = self()

      ReqTest.stub(MockAsyncHttp, fn conn ->
        if health_check?(conn) do
          ReqTest.json(conn, %{})
        else
          send(test_pid, {:submitted, conn.params})
          json(conn, 202, %{"statementHandle" => @handle})
        end
      end)

      start_repo()

      assert {:ok, @handle} =
               Snowflex.submit_async(AsyncRepo, "CALL proc(?)", ["arg"], query_tag: "my-tag")

      assert_received {:submitted, params}
      assert params["bindings"] == %{"1" => %{"type" => "TEXT", "value" => "arg"}}
      assert params["parameters"]["QUERY_TAG"] == "my-tag"
    end

    test "surfaces a Snowflake error body as a Snowflex.Error" do
      ReqTest.stub(MockAsyncHttp, fn conn ->
        if health_check?(conn) do
          ReqTest.json(conn, %{})
        else
          json(conn, 422, %{
            "code" => "000904",
            "message" => "SQL compilation error",
            "sqlState" => "42000"
          })
        end
      end)

      start_repo()

      assert {:error, error} = Snowflex.submit_async(AsyncRepo, "SELECT nope")
      assert error.message == "SQL compilation error"
      assert error.code == "000904"
      assert error.sql_state == "42000"
    end

    test "errors when Snowflake accepts the statement but returns no handle" do
      ReqTest.stub(MockAsyncHttp, fn conn ->
        if health_check?(conn) do
          ReqTest.json(conn, %{})
        else
          json(conn, 202, %{"unexpected" => "shape"})
        end
      end)

      start_repo()

      assert {:error, error} = Snowflex.submit_async(AsyncRepo, "CALL proc()")
      assert error.message =~ "no usable statementHandle"
    end

    test "errors when Snowflake returns a null statementHandle" do
      ReqTest.stub(MockAsyncHttp, fn conn ->
        if health_check?(conn) do
          ReqTest.json(conn, %{})
        else
          json(conn, 202, %{"statementHandle" => nil})
        end
      end)

      start_repo()

      assert {:error, error} = Snowflex.submit_async(AsyncRepo, "CALL proc()")
      assert error.message =~ "no usable statementHandle"
    end

    test "normalizes an iodata statement to a binary" do
      test_pid = self()

      ReqTest.stub(MockAsyncHttp, fn conn ->
        if health_check?(conn) do
          ReqTest.json(conn, %{})
        else
          send(test_pid, {:submitted, conn.params})
          json(conn, 202, %{"statementHandle" => @handle})
        end
      end)

      start_repo()

      assert {:ok, @handle} = Snowflex.submit_async(AsyncRepo, ["CALL ", "proc()"])

      assert_received {:submitted, params}
      assert params["statement"] == "CALL proc()"
    end

    test "sends the statement timeout to Snowflake in seconds" do
      test_pid = self()

      ReqTest.stub(MockAsyncHttp, fn conn ->
        if health_check?(conn) do
          ReqTest.json(conn, %{})
        else
          send(test_pid, {:submitted, conn.params})
          json(conn, 202, %{"statementHandle" => @handle})
        end
      end)

      start_repo()

      assert {:ok, @handle} =
               Snowflex.submit_async(AsyncRepo, "CALL proc()", [], timeout: :timer.seconds(30))

      assert_received {:submitted, params}
      assert params["timeout"] == 30
    end

    test "bounds the statement with :statement_timeout independently of the round-trip :timeout" do
      test_pid = self()

      ReqTest.stub(MockAsyncHttp, fn conn ->
        if health_check?(conn) do
          ReqTest.json(conn, %{})
        else
          send(test_pid, {:submitted, conn.params})
          json(conn, 202, %{"statementHandle" => @handle})
        end
      end)

      start_repo()

      assert {:ok, @handle} =
               Snowflex.submit_async(AsyncRepo, "CALL proc()", [],
                 timeout: :timer.seconds(30),
                 statement_timeout: :timer.hours(1)
               )

      # The server-side statement timeout follows :statement_timeout (1h -> 3600s),
      # not the 30s round-trip :timeout.
      assert_received {:submitted, params}
      assert params["timeout"] == 3600
    end

    test "falls back to :timeout for the statement timeout when :statement_timeout is absent" do
      test_pid = self()

      ReqTest.stub(MockAsyncHttp, fn conn ->
        if health_check?(conn) do
          ReqTest.json(conn, %{})
        else
          send(test_pid, {:submitted, conn.params})
          json(conn, 202, %{"statementHandle" => @handle})
        end
      end)

      start_repo()

      assert {:ok, @handle} =
               Snowflex.submit_async(AsyncRepo, "CALL proc()", [], timeout: :timer.seconds(45))

      assert_received {:submitted, params}
      assert params["timeout"] == 45
    end

    test "returns an error rather than exiting when the call times out" do
      ReqTest.stub(MockAsyncHttp, fn conn ->
        if health_check?(conn) do
          ReqTest.json(conn, %{})
        else
          Process.sleep(:infinity)
          json(conn, 202, %{})
        end
      end)

      start_repo()

      # The transport's GenServer.call must not exit: an uncaught exit escapes
      # Connection.handle_execute and DBConnection re-raises it, so the caller
      # would never see a Snowflex.Error.
      assert {:error, %Snowflex.Error{} = error} =
               Snowflex.submit_async(AsyncRepo, "CALL proc()", [], timeout: 100)

      assert error.message =~ "timed out"
    end
  end

  describe "statement_status/3" do
    test "maps 202 to :running and 200 to :succeeded" do
      ReqTest.stub(MockAsyncHttp, fn conn ->
        cond do
          health_check?(conn) -> ReqTest.json(conn, %{})
          conn.request_path == "/api/v2/statements/running-handle" -> json(conn, 202, %{})
          true -> json(conn, 200, %{"statementHandle" => @handle})
        end
      end)

      start_repo()

      assert {:ok, :running} = Snowflex.statement_status(AsyncRepo, "running-handle")
      assert {:ok, :succeeded} = Snowflex.statement_status(AsyncRepo, @handle)
    end

    test "reports a failed statement as an error" do
      ReqTest.stub(MockAsyncHttp, fn conn ->
        if health_check?(conn) do
          ReqTest.json(conn, %{})
        else
          json(conn, 422, %{
            "code" => "100183",
            "message" => "Division by zero",
            "sqlState" => "22012"
          })
        end
      end)

      start_repo()

      assert {:error, error} = Snowflex.statement_status(AsyncRepo, @handle)
      assert error.message == "Division by zero"
      assert error.code == "100183"
      assert error.sql_state == "22012"
    end

    test "does not request a partition when checking status" do
      test_pid = self()

      ReqTest.stub(MockAsyncHttp, fn conn ->
        if health_check?(conn) do
          ReqTest.json(conn, %{})
        else
          send(test_pid, {:status_query, conn.query_string})
          json(conn, 200, %{})
        end
      end)

      start_repo()

      assert {:ok, :succeeded} = Snowflex.statement_status(AsyncRepo, @handle)
      assert_received {:status_query, query_string}
      refute query_string =~ "partition"
    end
  end

  describe "cancel_statement/3" do
    test "posts to the cancel endpoint" do
      test_pid = self()

      ReqTest.stub(MockAsyncHttp, fn conn ->
        if health_check?(conn) do
          ReqTest.json(conn, %{})
        else
          send(test_pid, {:cancel, conn.method, conn.request_path})
          json(conn, 200, %{"statementHandle" => @handle})
        end
      end)

      start_repo()

      assert :ok = Snowflex.cancel_statement(AsyncRepo, @handle)
      assert_received {:cancel, "POST", "/api/v2/statements/#{@handle}/cancel"}
    end

    test "returns an error when the statement cannot be cancelled" do
      ReqTest.stub(MockAsyncHttp, fn conn ->
        if health_check?(conn) do
          ReqTest.json(conn, %{})
        else
          json(conn, 404, %{"code" => "002003", "message" => "Statement not found"})
        end
      end)

      start_repo()

      assert {:error, error} = Snowflex.cancel_statement(AsyncRepo, @handle)
      assert error.message == "Statement not found"
    end
  end

  describe "pool occupancy" do
    @tag :capture_log
    test "submit returns without waiting for the statement to finish" do
      test_pid = self()

      # Any GET on the handle blocks far longer than the assertion below
      # tolerates. Asserting on submit's own elapsed time is what makes this a
      # real regression guard: a second query afterwards would succeed either
      # way, because submit_async is awaited before that query even starts.
      ReqTest.stub(MockAsyncHttp, fn conn ->
        cond do
          health_check?(conn) ->
            ReqTest.json(conn, %{})

          conn.method == "GET" ->
            send(test_pid, :polled)
            Process.sleep(30_000)
            json(conn, 200, %{})

          conn.params["statement"] == "SELECT 2" ->
            json(conn, 200, %{
              "statementHandle" => "second",
              "resultSetMetaData" => %{"rowType" => [%{"name" => "X", "type" => "fixed"}]},
              "data" => [["2"]]
            })

          true ->
            json(conn, 202, %{"statementHandle" => @handle})
        end
      end)

      start_repo(pool_size: 1)

      {elapsed_us, submit_result} =
        :timer.tc(fn -> Snowflex.submit_async(AsyncRepo, "CALL long_proc()") end)

      assert {:ok, @handle} = submit_result

      # Polling to completion would have taken >= 30s.
      assert elapsed_us < 5_000_000,
             "submit_async blocked for #{div(elapsed_us, 1000)}ms; it must not wait for the statement"

      refute_received :polled

      # And the single pool slot is usable afterwards.
      assert {:ok, %Snowflex.Result{rows: [[2]]}} =
               AsyncRepo.query("SELECT 2", [], timeout: 2_000)
    end

    @tag :capture_log
    test "async ops reuse the connection held by an enclosing checkout" do
      ReqTest.stub(MockAsyncHttp, fn conn ->
        if health_check?(conn) do
          ReqTest.json(conn, %{})
        else
          json(conn, 202, %{"statementHandle" => @handle})
        end
      end)

      start_repo(pool_size: 1)

      # Asking the pool for a second connection here would queue behind the one
      # checkout already holds, burning the queue timeout and disconnecting the
      # outer checkout. Returning promptly proves the held connection is reused.
      {elapsed_us, result} =
        :timer.tc(fn ->
          AsyncRepo.checkout(fn -> Snowflex.submit_async(AsyncRepo, "CALL x()") end,
            timeout: 5_000
          )
        end)

      assert {:ok, @handle} = result

      assert elapsed_us < 2_000_000,
             "checkout + submit_async took #{div(elapsed_us, 1000)}ms; the pool slot was not reused"
    end
  end

  describe "fetch_result/3" do
    test "returns :running before completion and decoded rows after" do
      ReqTest.stub(MockAsyncHttp, fn conn ->
        cond do
          health_check?(conn) ->
            ReqTest.json(conn, %{})

          conn.request_path == "/api/v2/statements/running-handle" ->
            json(conn, 202, %{})

          true ->
            json(conn, 200, %{
              "statementHandle" => @handle,
              "resultSetMetaData" => %{
                "rowType" => [
                  %{"name" => "N", "type" => "fixed"},
                  %{"name" => "S", "type" => "text"}
                ],
                "partitionInfo" => [%{"rowCount" => 1}]
              },
              "data" => [["7", "hello"]]
            })
        end
      end)

      start_repo()

      assert {:ok, :running} = Snowflex.fetch_result(AsyncRepo, "running-handle")

      assert {:ok, %Snowflex.Result{} = result} = Snowflex.fetch_result(AsyncRepo, @handle)
      # Rows decode through the same path as a normal query, so "7" is an integer.
      assert result.rows == [[7, "hello"]]
      assert result.columns == ["N", "S"]
      assert result.num_rows == 1
    end

    test "merges multi-partition results" do
      ReqTest.stub(MockAsyncHttp, fn conn ->
        cond do
          health_check?(conn) ->
            ReqTest.json(conn, %{})

          conn.params["partition"] == "1" ->
            json(conn, 200, %{"data" => [["2"]]})

          true ->
            json(conn, 200, %{
              "statementHandle" => @handle,
              "resultSetMetaData" => %{
                "rowType" => [%{"name" => "N", "type" => "fixed"}],
                "partitionInfo" => [%{"rowCount" => 1}, %{"rowCount" => 1}]
              },
              "data" => [["1"]]
            })
        end
      end)

      start_repo()

      assert {:ok, %Snowflex.Result{rows: rows}} = Snowflex.fetch_result(AsyncRepo, @handle)
      assert rows == [[1], [2]]
    end

    test "surfaces a failed statement as an error" do
      ReqTest.stub(MockAsyncHttp, fn conn ->
        if health_check?(conn) do
          ReqTest.json(conn, %{})
        else
          json(conn, 422, %{"code" => "100183", "message" => "Division by zero"})
        end
      end)

      start_repo()

      assert {:error, error} = Snowflex.fetch_result(AsyncRepo, @handle)
      assert error.message == "Division by zero"
      assert error.code == "100183"
    end
  end

  describe "transports without async support" do
    defmodule NoAsyncTransport do
      @moduledoc false
      @behaviour Snowflex.Transport

      alias Snowflex.Result

      @impl Snowflex.Transport
      def start_link(_opts), do: Agent.start_link(fn -> :ok end)

      @impl Snowflex.Transport
      def execute_statement(_pid, _statement, _params, _opts), do: {:ok, %Result{}}

      @impl Snowflex.Transport
      def declare(_pid, _statement, _params, _opts), do: {:ok, 0}

      @impl Snowflex.Transport
      def fetch(_pid, _cursor, _opts), do: {:halt, %Result{}}

      @impl Snowflex.Transport
      def disconnect(_pid), do: :ok

      @impl Snowflex.Transport
      def deallocate(_pid), do: :ok

      @impl Snowflex.Transport
      def ping(_pid), do: {:ok, %Result{}}
    end

    test "report a descriptive error instead of crashing" do
      start_repo(transport: NoAsyncTransport)

      assert {:error, error} = Snowflex.submit_async(AsyncRepo, "CALL proc()")
      assert error.message =~ "does not implement submit_async/4"

      assert {:error, error} = Snowflex.statement_status(AsyncRepo, @handle)
      assert error.message =~ "does not implement statement_status/3"

      assert {:error, error} = Snowflex.fetch_result(AsyncRepo, @handle)
      assert error.message =~ "does not implement fetch_result/3"

      assert {:error, error} = Snowflex.cancel_statement(AsyncRepo, @handle)
      assert error.message =~ "does not implement cancel_statement/3"
    end
  end
end
