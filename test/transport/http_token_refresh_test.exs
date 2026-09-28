defmodule Snowflex.Transport.HttpTokenRefreshTest do
  use ExUnit.Case, async: false

  import Req.Test, only: [set_req_test_to_shared: 1]

  alias Plug.Conn
  alias Req.Test, as: ReqTest
  alias Snowflex.Transport.Http

  @handle "01b7e043-0206-7a43-0008-8b8300073d86"

  @moduletag :capture_log

  setup :set_req_test_to_shared

  defp private_key_path, do: Path.join(File.cwd!(), "test/fixtures/fake_private_key.pem")

  defp start_transport(opts) do
    start_link_supervised!(
      {Http,
       Keyword.merge(
         [
           account_name: "test_acc",
           username: "test_usr",
           private_key_path: private_key_path(),
           async_poll_interval: 10,
           req_options: [plug: {Req.Test, MockTokenHttp}]
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

  defp health_check?(%{params: %{"statement" => "SELECT 1"}}), do: true
  defp health_check?(_conn), do: false

  defp bearer(conn) do
    ["Bearer " <> token] = Conn.get_req_header(conn, "authorization")
    token
  end

  defp iat(token) do
    [_header, payload, _sig] = String.split(token, ".")

    payload
    |> Base.url_decode64!(padding: false)
    |> Jason.decode!()
    |> Map.fetch!("iat")
  end

  defp result_body(data \\ [["1"]]) do
    %{
      "statementHandle" => @handle,
      "data" => data,
      "resultSetMetaData" => %{
        "rowType" => [%{"name" => "X", "type" => "fixed"}],
        "partitionInfo" => [%{"rowCount" => length(data)}]
      }
    }
  end

  # Records the bearer token of every request and answers each with the
  # plain successful result set.
  defp stub_recording_tokens do
    test_pid = self()

    ReqTest.stub(MockTokenHttp, fn conn ->
      if health_check?(conn) do
        send(test_pid, {:health_check_token, bearer(conn)})
        ReqTest.json(conn, %{})
      else
        send(test_pid, {:call_token, bearer(conn)})
        json(conn, 200, result_body())
      end
    end)
  end

  # JWT `iat` has one-second resolution and RS256 signing is deterministic, so
  # a token re-signed within the same second is byte-identical. Cross a second
  # boundary before any call that must observe a newly signed token.
  defp next_second, do: Process.sleep(1_100)

  describe "timeout-aware refresh" do
    test "re-signs when the cached token cannot outlast the call" do
      stub_recording_tokens()
      pid = start_transport(token_lifetime: :timer.seconds(90))
      assert_received {:health_check_token, boot_token}

      next_second()

      # 80s timeout + 30s margin > 90s lifetime.
      assert {:ok, _result} =
               Http.execute_statement(pid, "SELECT 2", %{}, timeout: :timer.seconds(80))

      assert_received {:call_token, call_token}
      assert call_token != boot_token
      assert iat(call_token) > iat(boot_token)
    end

    test "reuses the cached token when it outlasts the call" do
      stub_recording_tokens()
      pid = start_transport(token_lifetime: :timer.seconds(90))
      assert_received {:health_check_token, boot_token}

      next_second()

      assert {:ok, _result} =
               Http.execute_statement(pid, "SELECT 2", %{}, timeout: :timer.seconds(10))

      assert_received {:call_token, ^boot_token}
    end

    test "always re-signs for an :infinity timeout" do
      stub_recording_tokens()
      pid = start_transport(token_lifetime: :timer.minutes(55))
      assert_received {:health_check_token, boot_token}

      next_second()

      assert {:ok, _result} = Http.execute_statement(pid, "SELECT 2", %{}, timeout: :infinity)

      assert_received {:call_token, call_token}
      assert iat(call_token) > iat(boot_token)
    end
  end

  describe "one-shot retry on an expired token (390198)" do
    @expired %{"code" => "390198", "message" => "JWT token is invalid."}

    # Serves the health check, then pops one canned {status, body} per
    # non-health-check request, recording {method, query_string, token}.
    defp stub_sequence(responses) do
      test_pid = self()
      {:ok, queue} = Agent.start_link(fn -> responses end)

      ReqTest.stub(MockTokenHttp, fn conn ->
        if health_check?(conn) do
          ReqTest.json(conn, %{})
        else
          send(test_pid, {:request, conn.method, conn.query_string, bearer(conn)})
          {status, body} = Agent.get_and_update(queue, &pop/1)
          json(conn, status, body)
        end
      end)
    end

    defp pop([next | rest]), do: {next, rest}

    defp requests(acc \\ []) do
      receive do
        {:request, method, query, token} -> requests([{method, query, token} | acc])
      after
        0 -> Enum.reverse(acc)
      end
    end

    test "retries an expired poll once against the same handle, without resubmitting" do
      stub_sequence([
        {202, %{"statementHandle" => @handle}},
        {401, @expired},
        {200, result_body()}
      ])

      pid = start_transport([])
      next_second()

      assert {:ok, %Snowflex.Result{rows: [["1"]]}} =
               Http.execute_statement(pid, "CALL slow()", %{}, [])

      assert [{"POST", _, submit_token}, {"GET", "", rejected}, {"GET", "", retried}] =
               requests()

      assert rejected == submit_token
      assert iat(retried) > iat(rejected)
    end

    test "retries an expired partition fetch once" do
      multi_partition =
        result_body()
        |> put_in(["resultSetMetaData", "partitionInfo"], [%{"rowCount" => 1}, %{"rowCount" => 1}])

      stub_sequence([
        {200, multi_partition},
        {401, @expired},
        {200, %{"data" => [["2"]]}}
      ])

      pid = start_transport([])
      next_second()

      assert {:ok, %Snowflex.Result{rows: [["1"], ["2"]]}} =
               Http.execute_statement(pid, "SELECT n", %{}, [])

      assert [{"POST", _, _}, {"GET", "partition=1", rejected}, {"GET", "partition=1", retried}] =
               requests()

      assert iat(retried) > iat(rejected)
    end

    test "retries only once" do
      stub_sequence([
        {202, %{"statementHandle" => @handle}},
        {401, @expired},
        {401, @expired}
      ])

      pid = start_transport([])

      assert {:error, %Snowflex.Error{code: "390198"}} =
               Http.execute_statement(pid, "CALL slow()", %{}, [])

      assert [{"POST", _, _}, {"GET", _, _}, {"GET", _, _}] = requests()
    end

    test "fetch_result/3 retries via the statement handle" do
      stub_sequence([{401, @expired}, {200, result_body()}])
      pid = start_transport([])
      next_second()

      assert {:ok, %Snowflex.Result{rows: [["1"]]}} = Http.fetch_result(pid, @handle, [])
      assert [{"GET", _, rejected}, {"GET", _, retried}] = requests()
      assert iat(retried) > iat(rejected)
    end

    test "statement_status/3 retries via the statement handle" do
      stub_sequence([{401, @expired}, {202, %{"statementHandle" => @handle}}])
      pid = start_transport([])

      assert {:ok, :running} = Http.statement_status(pid, @handle, [])
      assert [{"GET", _, _}, {"GET", _, _}] = requests()
    end

    test "cancel_statement/3 retries via the statement handle" do
      stub_sequence([{401, @expired}, {200, %{"statementHandle" => @handle}}])
      pid = start_transport([])

      assert {:ok, %Snowflex.Result{query_id: @handle}} =
               Http.cancel_statement(pid, @handle, [])

      assert [{"POST", _, _}, {"POST", _, _}] = requests()
    end
  end
end
