import Config

config :snowflex, Snowflex.ConnectionTest.SnowflakeConnection,
  worker: Snowflex.ConnectionTest.MockWorker,
  size: [
    min: 1,
    max: 1
  ]

config :snowflex, Snowflex.DBConnectionTest.SnowflakeDBConnection,
  worker: Snowflex.DBConnectionTest.MockWorker,
  pool_size: 3,
  connection: [
    server: "snowflex.us-east-8.snowflakecomputing.com",
    role: "DEV",
    warehouse: "CUSTOMER_DEV_WH"
  ]

config :logger, level: :warn

config :snowflex, ecto_repos: [Snowflex.SQLiteTestRepo]

config :snowflex, repo: Snowflex.SQLiteTestRepo

config :snowflex, Snowflex.SQLiteTestRepo,
  database: "test/snowflex_test_repo.sql",
  journal_mode: :delete,
  temp_store: :memory,
  pool: Ecto.Adapters.SQL.Sandbox
