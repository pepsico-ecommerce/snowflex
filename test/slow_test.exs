defmodule Snowflex.TimeoutTest do
  use ExUnit.Case

  setup do
    # Configure a test connection with:
    # - auto_commit: :off for transaction mode
    # - Shorter timeout for faster test execution
    config = [
      connection: [
        auto_commit: :off,
        server: "test-server",
        role: "TEST",
        warehouse: "TEST_WH"
      ],
      # Define this below
      worker: SlowMockWorker
    ]

    {:ok, config: config}
  end

  defmodule SlowMockWorker do
    # Implement Snowflex.Client behavior but make commit/rollback slow
    def commit(_pid, :rollback, _opts) do
      # Simulate slow rollback
      # Longer than default 60s timeout
      Process.sleep(61_000)
      :ok
    end

    # Implement other required callbacks...
  end

  test "timeout during rollback", %{config: config} do
    {:ok, conn} = Snowflex.Connection.start_link(config)

    # Start a transaction
    {:ok, _} = Snowflex.query(conn, "BEGIN TRANSACTION")

    # Try to rollback - should timeout
    assert catch_exit(Snowflex.query(conn, "ROLLBACK"))
  end
end
