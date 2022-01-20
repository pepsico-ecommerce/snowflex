import Config

config :snowflex, driver: :odbc

# Import environment specific config. This must remain at the bottom
# of this file so it overrides the configuration defined above.
import_config "#{Mix.env()}.exs"

# Import a "secret" configuration for the build environment if one exists. These
# configurations are unique to each developer and are intended to allow local
# configuration as needed.
if File.exists?("config/#{Mix.env()}.secret.exs") do
  import_config "#{Mix.env()}.secret.exs"
end
