**THIS IS A WORK IN PROGRESS. USE AT YOUR OWN RISK.**

# Snowflex ❄💪

[![Module Version](https://img.shields.io/hexpm/v/snowflex.svg)](https://hex.pm/packages/snowflex)
[![Hex Docs](https://img.shields.io/badge/hex-docs-lightgreen.svg)](https://hexdocs.pm/snowflex/)
[![Total Download](https://img.shields.io/hexpm/dt/snowflex.svg)](https://hex.pm/packages/snowflex)
[![License](https://img.shields.io/hexpm/l/snowflex.svg)](https://github.com/pepsico-ecommerce/snowflex/blob/master/LICENSE)
[![Last Updated](https://img.shields.io/github/last-commit/pepsico-ecommerce/snowflex.svg)](https://github.com/pepsico-ecommerce/snowflex/commits/master)

This application encapsulates an ODBC connection pool for connecting to the Snowflake data warehouse.

## Setup

The following config options can be set:

```elixir
config :snowflex,
  driver: "/path/to/my/ODBC/driver" # defaults to "/usr/lib/snowflake/odbc/lib/libSnowflake.so"
```

Connection pools are not automatically started for you. You will need to define and establish each connection pool in your application module. Configuration values related to connection timeouts and the mapping of `:null` query values can be set here.

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

The odbc driver will, by default, return `:null` for empty values returned from snowflake queries.
This will be converted to `nil` by default by Snowflex. A configuration value `map_nulls_to_nil?`
can be set to `false` if you do not desire this behavior.

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

## Caveats

If you are planning to connect to the Snowflake warehouse, your local Erlang instance
must have ODBC enabled. The erlang installed by Homebrew does NOT have ODBC support. The `asdf`
version of erlang does have ODBC support. You will also need the Snowflake ODBC driver installed
on your machine. You can download this from https://sfc-repo.snowflakecomputing.com/odbc/index.html.

### Apple Silicon

Snowflake has a native `macaarch64 driver` available from https://sfc-repo.snowflakecomputing.com/odbc/macaarch64/index.html. However Erlang is unable to find the `unixodbc` files by default after Homebrew [changed their installation directory](https://github.com/Homebrew/brew/issues/9177) from `/usr/local` to `/opt/homebrew`.

We can build Erlang with `asdf` and ensure the correct files included to make sure `odbc.app` is available when running Elixir.

We will need [asdf](https://asdf-vm.com) and [Homebrew](https://brew.sh) installed.

Next, we should first remove any previous installations or builds of Elixir or Erlang to make sure they are not incorrectly targeted by `mix` when we run our applicatoin. This can be done like so:

``` sh
brew uninstall elixir
brew uninstall erlang
asdf uninstall erlang
rm ~/.asdf/plugins/erlang/kerl-home/otp_builds
rm ~/.asdf/plugins/erlang/kerl-home/otp_installations
```

We can now get the neccesary ODBC and OpenSSL files from Brew, set their correct locations in the environment, and build Erlang and Elixir with `asdf` like so:

``` sh
brew install unixodbc
brew install openssl@1.1
export KERL_CONFIGURE_OPTIONS="--with-odbc=$(brew --prefix unixodbc) --with-ssl=$(brew --prefix openssl@1.1)"
export CC="/usr/bin/gcc -I$(brew --prefix unixodbc)/include"
export LDFLAGS="-L$(brew --prefix unixodbc)/lib"
asdf install erlang
asdf install elixir
unset KERL_CONFIGURE_OPTIONS
unset CC
unset LDFLAGS
```

You will then need to add the following to `/opt/snowflake/snowflakeodbc/lib/simba.snowflake.ini`
```
ODBCInstLib=/opt/homebrew/Cellar/unixodbc/2.3.11/lib/libodbcinst.dylib
```

And finally ensure that your elixir config has the correct driver location

``` elixir
config :snowflex, driver: "/opt/snowflake/snowflakeodbc/lib/libSnowflake.dylib"
```

## Installation

The package can be installed by adding `:snowflex` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:snowflex, "~> 0.5.1"}
  ]
end
```

## DBConnection Support

[DBConnection](https://github.com/elixir-ecto/db_connection) support is currently in experimental phase, setting it up is very similar to current implementation with the expection of configuration options and obtaining the same results will require an extra step:

### Configuration:

Setting a Module to hold the connection is very similar, but instead you'll use `Snowflex.DBConnection`:

Example:

```elixir
defmodule MyApp.SnowflakeConnection do
  use Snowflex.DBConnection,
    otp_app: :my_app,
    timeout: :timer.minutes(5)
end
```

```elixir
config :my_app, MyApp.SnowflakeConnection,
  pool_size: 5, # the connection pool size
  worker: MyApp.CustomWorker, # defaults to Snowflex.DBConnection.Server
  connection: [
      role: "PROD",
      warehouse: System.get_env("SNOWFLAKE_POS_WH"),
      uid: System.get_env("SNOWFLAKE_POS_UID"),
      pwd: System.get_env("SNOWFLAKE_POS_PWD")
    ]
```

### Usage:

After setup, you can use your connection to query:

```elixir
alias Snowflex.DBConnection.Result

{:ok, %Result{} = result} = MyApp.SnowflakeConnection.execute("my query")
{:ok, %Result{} = result} = MyApp.SnowflakeConnection.execute("my query", ["my params"])
```

As you can see we now receive an `{:ok, result}` tuple, to get results as expected with current implementation, we need to call `process_result/1`:

```elixir
alias Snowflex.DBConnection.Result

{:ok, %Result{} = result} = MyApp.SnowflakeConnection.execute("my query")

[%{"col" => 1}, %{"col" => 2}] = SnowflakeDBConnection.process_result(result)
```

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
