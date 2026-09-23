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
      assert error.message =~ "no statementHandle"
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
    test "a submitted statement does not hold the only pool slot while it runs" do
      test_pid = self()

      # The submit answers 202 immediately. Any GET on the handle blocks for
      # longer than the follow-up query's timeout, so if the transport were
      # polling to completion inside the checked-out connection, the second
      # query could not get a slot and would time out.
      ReqTest.stub(MockAsyncHttp, fn conn ->
        cond do
          health_check?(conn) ->
            ReqTest.json(conn, %{})

          conn.method == "GET" ->
            send(test_pid, :polled)
            Process.sleep(5_000)
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

      assert {:ok, @handle} = Snowflex.submit_async(AsyncRepo, "CALL long_proc()")

      assert {:ok, %Snowflex.Result{rows: [[2]]}} =
               AsyncRepo.query("SELECT 2", [], timeout: 2_000)

      refute_received :polled
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

      assert {:error, error} = Snowflex.cancel_statement(AsyncRepo, @handle)
      assert error.message =~ "does not implement cancel_statement/3"
    end
  end
end
