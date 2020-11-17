**THIS IS A WORK IN PROGRESS. USE AT YOUR OWN RISK.**

# Snowflex ❄💪

This application encapsulates an ODBC connection pool for connecting to the Snowflake data warehouse.

## Usage

The following config options can be set:

```elixir
config :snowflex,
  driver: "/path/to/my/ODBC/driver" # defaults to "/usr/lib/snowflake/odbc/lib/libSnowflake.so")
```

Connection pools are not automatically started for you. You will need to establish each connection pool in your application module. Example configuration:

```elixir
import Config

# ...

config :my_app, :data_warehouses,
  point_of_sale: [
    name: :point_of_sale,
    connection: [
      role: "PROD",
      warehouse: System.get_env("SNOWFLAKE_POS_WH"),
      uid: System.get_env("SNOWFLAKE_POS_UID"),
      pwd: System.get_env("SNOWFLAKE_POS_PWD")
    ]
  ],
  advertising: [
    name: :advertising,
    worker_module: MyApp.MockWorker # defaults to Snowflex.Worker (change for testing/development)
    connection: [
      role: "PROD",
      warehouse: System.get_env("SNOWFLAKE_ADVERTISING_WH"),
      uid: System.get_env("SNOWFLAKE_ADVERTISING_UID"),
      pwd: System.get_env("SNOWFLAKE_ADVERTISING_PWD")
    ]
  ]
```

Then, in your application module, you would source the configuration like this:

```elixir
def MyApp.Application do
  use Application

  def start(_type, _args) do

    warehouses = Application.get_env(:myapp, :data_warehouses)
    pos = Keyword.get(warehouses, :point_of_sale)
    advertising = Keyword.get(warehouses, :advertising)

    children = [
      {Snowflex.ConnectionPool, pos},
      {Snowflex.ConnectionPool, advertising}
    ]

    opts = [strategy: :one_for_one, name: MyApp.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
```

This setup allows us to support multiple connection pools to different warehouses.

## Caveats

If you are planning to connect to the Snowflake warehouse, your local Erlang instance
must have ODBC enabled. The erlang installed by Homebrew does NOT have ODBC support. The `asdf`
version of erlang does have ODBC support. You will also need the Snowflake ODBC driver installed
on your machine. You can download this from https://sfc-repo.snowflakecomputing.com/odbc/index.html.

## Installation

The package can be installed by adding `snowflex` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:snowflex, "~> 0.1.1"}
  ]
end
```
