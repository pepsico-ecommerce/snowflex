defmodule Snowflex.Transport.HttpTokenRefreshTest do
  use ExUnit.Case, async: false

  import Req.Test, only: [set_req_test_to_shared: 1]

  alias Plug.Conn
  alias Req.Test, as: ReqTest
  alias Snowflex.Transport.Http

  @handle "01b7e043-0206-7a43-0008-8b8300073d86"

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
end
