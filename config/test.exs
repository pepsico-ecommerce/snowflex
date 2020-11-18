import Config

config :snowflex, Snowflex.ConnectionTest.SnowflakeConnection,
  worker: Snowflex.ConnectionTest.MockWorker,
  size: [
    min: 1,
    max: 1
  ]
