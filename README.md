**THIS IS A WORK IN PROGRESS. USE AT YOUR OWN RISK.**

[![Published on Hex](https://img.shields.io/hexpm/v/snowflex)](https://hex.pm/packages/snowflex)
[![License Info](https://img.shields.io/hexpm/l/snowflex)](https://github.com/pepsico-ecommerce/snowflex/blob/master/LICENSE)

# Snowflex ❄💪

This application includes a functional interface, as well as a `:db_connection`
driver for connecting to a Snowflake data warehouse. By default, this is backed
by an `:odbc` connection pool, but it may also use Snowflake's HTTP API.

## ODBC Setup

For the `:odbc` transport, make sure your Erlang runtime includes the `:odbc`
application. The following config options can be set:

```elixir
config :snowflex,
  transport: :odbc, # not required, since this is the default transport
  driver: "/path/to/my/ODBC/driver" # defaults to "/usr/lib/snowflake/odbc/lib/libSnowflake.so"
```

For the `:http` transport, you'll also need to install the optional `:tesla`
and `:jason` dependencies, along with a Tesla-compatible network adapter
(`:hackney` is recommended):

```elixir
# config.exs
config :snowflex, transport: :http
config :tesla, adapter: Tesla.Adapter.Hackney

# your_app/mix.exs
def deps do
  [
    # ...
    {:tesla, "~> 1.4"},
    {:jason, "~> 1.3"},
    {:hackney, "~> 1.18"}
  ]
end
```

In order to use `Snowflex.cast_results/2`, you'll also need to install the
optional `:ecto` dependency:

```elixir
def deps do
  [
    # ...
    {:ecto, "~> 3.0"}
  ]
end
```

Connection pools are not automatically started for you. You will need to define
and establish each connection pool in your application module. Configuration
values related to connection timeouts and the mapping of `:null` query values
can be set here.

First, create a module to hold your connection information:

```elixir
defmodule MyApp.SnowflakeConnection do
  use Snowflex.Connection,
    otp_app: :my_app,
    timeout: :timer.minutes(20),
    map_nulls_to_nil?: true
end
```

Define your configuration:

```elixir
import Config

# ...

config :my_app, MyApp.SnowflakeConnection,
  connection: [
    role: "PROD",
    warehouse: System.get_env("SNOWFLAKE_POS_WH"),
    uid: System.get_env("SNOWFLAKE_POS_UID"),
    pwd: System.get_env("SNOWFLAKE_POS_PWD")
  ]

 # you may define multiple connection modules
 config :my_app, MyApp.MyOtherSnowflakeConnection,
  worker: MyApp.MockWorker # defaults to Snowflex.Worker (change for testing/development)
  connection: [
    role: "PROD",
    warehouse: System.get_env("SNOWFLAKE_ADVERTISING_WH"),
    uid: System.get_env("SNOWFLAKE_ADVERTISING_UID"),
    pwd: System.get_env("SNOWFLAKE_ADVERTISING_PWD")
  ]
```

The odbc driver will, by default, return `:null` for empty values returned from
Snowflake queries. This will be converted to `nil` by default by Snowflex. A
configuration value `map_nulls_to_nil?` can be set to `false` if you do not
desire this behavior.

Then, in your application module, you would start your connection:

```elixir
def MyApp.Application do
  use Application

  def start(_type, _args) do

    children = [
      MyApp.SnowflakeConnection,
      MyApp.MyOtherSnowflakeConnection
    ]

    opts = [strategy: :one_for_one, name: MyApp.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
```

## Connection Config

The following `:connection` configuration options are available:

```elixir
config :my_app, MyApp.SnowflakeConnection,
  connection: [
    # any transport
    warehouse: "EXAMPLE_WH",
    role: "READ_ONLY",

    # `:odbc` transport
    server: "subdomain.domain.tld",
    uid: "user",
    pwd: "pass",

    # `:http` transport
    account: "account_identifier",
    key: "rsa_private_key"
  ]
```

For more information on configuring key pair authentication for the `:http`
transport, see [here][key-pair-auth].

[key-pair-auth]: https://docs.snowflake.com/en/developer-guide/sql-api/guide.html#label-sql-api-authenticating-key-pair

## Caveats

If you are planning to connect to the Snowflake warehouse, your local Erlang
instance must have ODBC enabled. The erlang installed by Homebrew does NOT have
ODBC support. The `asdf` version of erlang does have ODBC support. You will also
need the Snowflake ODBC driver installed on your machine. You can download this
from https://sfc-repo.snowflakecomputing.com/odbc/index.html.

## Installation

The package can be installed by adding `snowflex` to your list of dependencies
in `mix.exs`:

```elixir
def deps do
  [
    {:snowflex, "~> 0.4.4"}
  ]
end
```
