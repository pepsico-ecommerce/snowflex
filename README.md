# Snowflex ❄💪

**THIS IS A WORK IN PROGRESS. USE AT YOUR OWN RISK.**

[![Module Version](https://img.shields.io/hexpm/v/snowflex.svg)](https://hex.pm/packages/snowflex)
[![Hex Docs](https://img.shields.io/badge/hex-docs-lightgreen.svg)](https://hexdocs.pm/snowflex/)
[![Total Download](https://img.shields.io/hexpm/dt/snowflex.svg)](https://hex.pm/packages/snowflex)
[![License](https://img.shields.io/hexpm/l/snowflex.svg)](https://github.com/pepsico-ecommerce/snowflex/blob/master/LICENSE)
[![Last Updated](https://img.shields.io/github/last-commit/pepsico-ecommerce/snowflex.svg)](https://github.com/pepsico-ecommerce/snowflex/commits/master)

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

If you are planning to connect to the Snowflake warehouse, your local Erlang instance
must have ODBC enabled. The erlang installed by Homebrew does NOT have ODBC support. The `asdf`
version of erlang does have ODBC support. You will also need the Snowflake ODBC driver installed
on your machine. You can download this from <https://sfc-repo.snowflakecomputing.com/odbc/index.html>.

### Apple Silicon

Snowflake has a native `macaarch64 driver` available from <https://sfc-repo.snowflakecomputing.com/odbc/macaarch64/index.html>. However Erlang is unable to find the `unixodbc` files by default after Homebrew [changed their installation directory](https://github.com/Homebrew/brew/issues/9177) from `/usr/local` to `/opt/homebrew`.

We can build Erlang with `asdf` and ensure the correct files included to make sure `odbc.app` is available when running Elixir.

We will need [asdf](https://asdf-vm.com) and [Homebrew](https://brew.sh) installed.

Next, we should first remove any previous installations or builds of Elixir or Erlang to make sure they are not incorrectly targeted by `mix` when we run our applicatoin. This can be done like so:

```sh
brew uninstall elixir
brew uninstall erlang
asdf uninstall erlang
rm ~/.asdf/plugins/erlang/kerl-home/otp_builds
rm ~/.asdf/plugins/erlang/kerl-home/otp_installations
```

We can now get the neccesary ODBC and OpenSSL files from Brew, set their correct locations in the environment, and build Erlang and Elixir with `asdf` like so:

```sh
brew install unixodbc
brew install openssl@1.1
export KERL_CONFIGURE_OPTIONS="--with-odbc=$(brew --prefix unixodbc) --with-ssl=$(brew --prefix openssl@1.1)"
export CC="/usr/bin/gcc -I$(brew --prefix unixodbc)/include"
export LDFLAGS="-L$(brew --prefix unixodbc)/lib"
asdf install erlang
asdf install elixir
unset KERL_CONFIGURE_OPTIONS
unset CC
```

You will then need to add the following to `/opt/snowflake/snowflakeodbc/lib/simba.snowflake.ini`

```ini
ODBCInstLib=/opt/homebrew/Cellar/unixodbc/2.3.11/lib/libodbcinst.dylib
```

And finally ensure that your elixir config has the correct driver location

```elixir
config :snowflex, driver: "/opt/snowflake/snowflakeodbc/lib/libSnowflake.dylib"
```

## Installation

The package can be installed by adding `:snowflex` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:snowflex, "~> 1.0.0"}
  ]
end
```

## Useage

An Ecto Adapter is provided to allow for useage similar to any other SQL backed adapter.

Simply declare an Ecto Repo using the Snowlfex adapter, define Ecto Schema modules and use standard Ecto functions.

```elixir
defmodule MyApp.Repo do
  use Ecto.Repo,
    otp_app: :my_app,
    adapter: Snowflex.EctoAdapter
end
```

```elixir
defmodule MyApp.Schema do
  use Ecto.Schema

  schema "schema" do
    field(:x, :integer)
    field(:y, :integer)
    field(:z, :integer)
  end
end
```

```elixir
MyApp.Repo.all(MyApp.Schema)
```

## Testing

Testing Ecto Schemas without connecting to a live Snowflake database is made
possible through swapping out the Ecto Adapter. A test repo which uses SQLite is
provided.

The largest difficulty with using another adapter is that there will be no
migrations to get the test repo in a useable state for testing. This is solved
by the `generate_migrations/2` macro in the `MigrationGenerator` module. The
test repo must also be created and dropped before each run of the test suite to
allow the generated migrations to run from a blank state.

Install `ecto_sqlite3` in `mix.exs`:

```elixir
      {:ecto_sqlite3, "~> 0.8", only: [:test]},
```

And update `test/test_helper.exs` file as follows:

```elixir
require Snowflex.MigrationGenerator

opts = [strategy: :one_for_one, name: Snowflex.Supervisor]
Supervisor.start_link([Snowflex.SQLiteTestRepo], opts)

Snowflex.SQLiteTestRepo.__adapter__().storage_up(Snowflex.SQLiteTestRepo.config())

Snowflex.MigrationGenerator.generate_migrations(Snowflex.SQLiteTestRepo, [
  TestSchema,
  TestSchema2,
  TestSchema3
])

ExUnit.start()

ExUnit.after_suite(fn _ ->
  Snowflex.SQLiteTestRepo.__adapter__().storage_down(Snowflex.SQLiteTestRepo.config())
end)
```

Refer to `test/snowflex_sqlite_test.exs` for useage.

## Copyright and License

Copyright (c) 2020 PepsiCo, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0).

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
