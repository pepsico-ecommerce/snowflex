defmodule Snowflex.MockReqResponsesTest do
  use ExUnit.Case, async: false

  import Req.Test, only: [set_req_test_to_shared: 1]
  import ExUnit.CaptureLog

  alias Plug.Conn
  alias Req.Test, as: ReqTest

  require Logger

  setup :set_req_test_to_shared

  defmodule TestSnowflakeRepo do
    use Ecto.Repo,
      otp_app: :snowflex,
      adapter: Snowflex
  end

  describe "Error Handling for Req raised errors" do
    setup do
      # Configure Logger to accept Snowflex metadata keys
      Logger.configure_backend(:console,
        metadata: [
          :snowflex_account_name,
          :snowflex_username,
          :snowflex_warehouse,
          :snowflex_role,
          :snowflex_database,
          :snowflex_schema,
          :snowflex_query_id,
          :snowflex_statement
        ]
      )

      private_key_path = Path.join(File.cwd!(), "test/fixtures/fake_private_key.pem")

      ReqTest.stub(MockHttp, fn
        %{params: %{"statement" => "SELECT 1"}} = conn ->
          # Health check
          ReqTest.json(conn, %{})

        %{params: %{"statement" => _statement}} = conn ->
          # Response to mock statement with error including statementHandle
          conn
          |> Conn.put_resp_content_type("application/json")
          |> Conn.send_resp(
            529,
            Jason.encode!(%{
              "code" => "529",
              "message" => "Server too busy. Please retry.",
              "statementHandle" => "01b7e043-0206-7a43-0008-8b8300073d86"
            })
          )
      end)

      Req.default_options(plug: {Req.Test, MockHttp})

      start_link_supervised!(
        {TestSnowflakeRepo,
         [
           account_name: "test_acc",
           username: "test_usr",
           private_key_path: private_key_path,
           public_key_fingerprint:
             "4dfd2c71b73c0c5a600c5e96004ca52204dfd74632e8e53738770538f7b8af5c",
           role: "fake_role",
           warehouse: "fake_warehouse"
         ]}
      )

      :ok
    end

    test "http connection errors from ecto query_many should surface granular errors" do
      # Capture logs with metadata to verify Logger.metadata is working
      logs =
        capture_log([metadata: :all], fn ->
          assert_raise Snowflex.Error, "Server too busy. Please retry.", fn ->
            TestSnowflakeRepo.query_many!("SELECT * from THIS_MUST_ERROR")
          end
        end)

      # Assert all expected metadata is present in logs
      assert logs =~ "snowflex_account_name=test_acc"
      assert logs =~ "snowflex_username=test_usr"
      assert logs =~ "snowflex_warehouse=fake_warehouse"
      assert logs =~ "snowflex_role=fake_role"
      assert logs =~ "snowflex_statement=SELECT * from THIS_MUST_ERROR"
      assert logs =~ "snowflex_query_id=01b7e043-0206-7a43-0008-8b8300073d86"

      # Verify the error log itself appears
      assert logs =~ "QUERY ERROR"
    end

    test "http connection errors from ecto .all() should surface granular errors" do
      # Capture logs with metadata to verify Logger.metadata is working
      logs =
        capture_log([metadata: :all], fn ->
          assert_raise Snowflex.Error, "Server too busy. Please retry.", fn ->
            TestSnowflakeRepo.all(TestSchema)
          end
        end)

      # Assert all expected metadata is present in logs
      assert logs =~ "snowflex_account_name=test_acc"
      assert logs =~ "snowflex_username=test_usr"
      assert logs =~ "snowflex_warehouse=fake_warehouse"
      assert logs =~ "snowflex_role=fake_role"

      assert logs =~
               "snowflex_statement=SELECT s0.id, s0.x, s0.y, s0.z, s0.meta FROM schema AS s0"

      assert logs =~ "snowflex_query_id=01b7e043-0206-7a43-0008-8b8300073d86"

      # Verify the error log itself appears
      assert logs =~ "QUERY ERROR"
    end
  end
end
