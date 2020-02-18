**THIS IS A WORK IN PROGRESS. USE AT YOUR OWN RISK.**

# Snowflex ❄💪

This application encapsulates an ODBC connection pool for connecting to the Snowflake data warehouse.

The following config options need to be set:

```elixir
config :snowflex, :connection,
       driver: <path to driver library>,
       server: <URL to server>,
       uid: <user id>,
       pwd: <password>,
       role: <Snowflake role>,
       warehouse:  <Snowflake warehouse>

config :snowflex, :pool,
  pool_size: <pool size to pass to poolboy>,
  overflow: <pool overflow to pass to poolboy>

config :snowflex, worker: Snowflex.MockWorker
```

The last line is optional and designed to use in test/production. It returns canned data and does
not connect to the data warehouse. If you want to connect to this real warehouse, omit this line
or set it to `Snowflex.Worker`

NOTE: If you are planning to connect to the Snowflake warehouse, your local Erlang instance
must have ODBC enabled. The erlang installed by Homebrew does NOT have ODBC support. The `asdf`
version of erlang does have ODBC support. You will also need the Snowflake ODBC driver installed
on your machine. You can download this from https://sfc-repo.snowflakecomputing.com/odbc/index.html.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `snowflex` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:snowflex, "~> 0.0.1"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at [https://hexdocs.pm/snowflex](https://hexdocs.pm/snowflex).
