import Config

# capture all logs
config :logger, level: :debug

# but only output warnings+ to console.
config :logger, :console, level: :warning
