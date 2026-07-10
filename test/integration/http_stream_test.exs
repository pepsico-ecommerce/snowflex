defmodule Snowflex.HttpStreamTest do
  # Full-stack streaming through Ecto.Repo.stream/2 and Snowflex.stream_query/5:
  # Repo → DBConnection cursor → Snowflex.Transport.Http → stubbed Snowflake
  # SQL API.
  use ExUnit.Case, async: false

  import Ecto.Query, only: [from: 2]
  import Req.Test, only: [set_req_test_to_shared: 1]

  alias Plug.Conn
  alias Req.Test, as: ReqTest
  alias Snowflex.Result

  setup :set_req_test_to_shared

  defmodule TestSnowflakeRepo do
    use Ecto.Repo,
      otp_app: :snowflex,
      adapter: Snowflex
  end

  @sync_handle "01b7e043-0206-7a43-0008-8b8300073d01"
  @async_handle "01b7e043-0206-7a43-0008-8b8300073d02"

  # Three partitions of one row each; every streamed partition is fetched via
  # GET ?partition=N (partition 0 included — declare discards the inline data).
  # Values arrive as strings, like the real SQL API sends them.
  defp partition_data(n), do: [["p#{n}", "#{n}"]]

  defp completed_body(handle) do
    %{
      "statementHandle" => handle,
      "resultSetMetaData" => %{
        "partitionInfo" => [
          %{"rowCount" => 1},
          %{"rowCount" => 1},
          %{"rowCount" => 1}
        ],
        "rowType" => [
          %{"name" => "N", "type" => "text"},
          %{"name" => "CNT", "type" => "fixed", "scale" => 0}
        ]
      },
      "data" => partition_data(0)
    }
  end

  setup do
    private_key_path = Path.join(File.cwd!(), "test/fixtures/fake_private_key.pem")

    ReqTest.stub(SnowflexStreamStub, fn
      %{method: "POST", params: %{"statement" => statement}} = conn ->
        case statement do
          "SELECT 1" ->
            # Connection health check on transport startup
            ReqTest.json(conn, %{})

          "SELECT ASYNC" ->
            # Long-running statement: Snowflake answers 202 and the client
            # must poll GET /statements/{handle} until the result set exists
            conn
            |> Conn.put_resp_content_type("application/json")
            |> Conn.send_resp(202, Jason.encode!(%{"statementHandle" => @async_handle}))

          _streamed ->
            # Any other statement (raw "SELECT STREAMED" or Ecto-generated
            # SQL) completes immediately with a three-partition result set
            ReqTest.json(conn, completed_body(@sync_handle))
        end

      %{method: "GET", params: %{"partition" => partition}} = conn ->
        ReqTest.json(conn, %{"data" => partition_data(partition)})

      %{method: "GET"} = conn ->
        # The only partition-less GET here is the poll for the async
        # statement's completion
        ReqTest.json(conn, completed_body(@async_handle))
    end)

    start_link_supervised!(
      {TestSnowflakeRepo,
       [
         account_name: "test_acc",
         username: "test_usr",
         private_key_path: private_key_path,
         role: "fake_role",
         warehouse: "fake_warehouse",
         pool_size: 1,
         async_poll_interval: 10,
         req_options: [plug: {Req.Test, SnowflexStreamStub}]
       ]}
    )

    :ok
  end

  test "stream_query/5 streams exactly one Result per partition with typed decoding" do
    results =
      Snowflex.stream_query(TestSnowflakeRepo, "SELECT STREAMED", [], [], &Enum.to_list/1)

    # The "fixed" CNT column decodes to an integer, exactly as execute does,
    # and the cursor halts with the final partition — no trailing empty result
    assert [
             %Result{columns: ["N", "CNT"], rows: [["p0", 0]]},
             %Result{columns: ["N", "CNT"], rows: [["p1", 1]]},
             %Result{columns: ["N", "CNT"], rows: [["p2", 2]]}
           ] = results
  end

  test "stream_query/5 flat_maps into a plain row stream" do
    rows =
      Snowflex.stream_query(TestSnowflakeRepo, "SELECT STREAMED", fn stream ->
        stream
        |> Stream.flat_map(fn %Result{rows: rows} -> rows || [] end)
        |> Enum.to_list()
      end)

    assert rows == [["p0", 0], ["p1", 1], ["p2", 2]]
  end

  test "stream_query/5 polls async (202) statements to completion before streaming" do
    rows =
      Snowflex.stream_query(TestSnowflakeRepo, "SELECT ASYNC", fn stream ->
        stream
        |> Stream.flat_map(fn %Result{rows: rows} -> rows || [] end)
        |> Enum.to_list()
      end)

    assert rows == [["p0", 0], ["p1", 1], ["p2", 2]]
  end

  test "stream_query/5 inside Repo.checkout/2 reuses the held connection" do
    # pool_size is 1, so this deadlocks (then times out) if stream_query
    # checks out a second connection instead of reusing checkout's
    rows =
      TestSnowflakeRepo.checkout(fn ->
        Snowflex.stream_query(TestSnowflakeRepo, "SELECT STREAMED", fn stream ->
          stream
          |> Stream.flat_map(fn %Result{rows: rows} -> rows || [] end)
          |> Enum.to_list()
        end)
      end)

    assert rows == [["p0", 0], ["p1", 1], ["p2", 2]]
  end

  test "Repo.stream/2 streams an Ecto queryable lazily inside Repo.checkout/2" do
    rows =
      TestSnowflakeRepo.checkout(fn ->
        from(t in "big_table", select: [t.n, t.cnt])
        |> TestSnowflakeRepo.stream()
        |> Enum.to_list()
      end)

    assert rows == [["p0", 0], ["p1", 1], ["p2", 2]]
  end

  test "Repo.stream/2 halts the cursor early without consuming every partition" do
    rows =
      TestSnowflakeRepo.checkout(fn ->
        from(t in "big_table", select: [t.n, t.cnt])
        |> TestSnowflakeRepo.stream()
        |> Enum.take(1)
      end)

    assert rows == [["p0", 0]]
  end

  test "Repo.stream/2 raises when enumerated outside Repo.checkout/2" do
    stream = TestSnowflakeRepo.stream(from(t in "big_table", select: [t.n, t.cnt]))

    assert_raise RuntimeError, ~r/outside of Ecto\.Repo\.checkout\/2/, fn ->
      Enum.to_list(stream)
    end
  end
end
