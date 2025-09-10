# Snowflex ❄💪

[![Module Version](https://img.shields.io/hexpm/v/snowflex.svg)](https://hex.pm/packages/snowflex)
[![Hex Docs](https://img.shields.io/badge/hex-docs-lightgreen.svg)](https://hexdocs.pm/snowflex/)
[![Total Download](https://img.shields.io/hexpm/dt/snowflex.svg)](https://hex.pm/packages/snowflex)
[![License](https://img.shields.io/hexpm/l/snowflex.svg)](https://github.com/pepsico-ecommerce/snowflex/blob/master/LICENSE)
[![Last Updated](https://img.shields.io/github/last-commit/pepsico-ecommerce/snowflex.svg)](https://github.com/pepsico-ecommerce/snowflex/commits/master)

- [Requirements](#requirements)
- [Configuration](#configuration)
- [Query Tagging](#query-tagging)
- [Type Support](#type-support)
- [Limitations/Considerations](#limitationsconsiderations)
- [Migration from ODBC](#migration-from-odbc)
- [Testing in your project](#testing-in-your-project)

---

> [!NOTE]
>
> This README is for the unreleased main branch, please reference the [official documentation on
> hexdocs][hexdoc] for the latest stable release.

[hexdoc]: https://hexdocs.pm/snowflex/readme.html

<!-- MDOC -->

This adapter implements the following Ecto behaviours:

- `Ecto.Adapter` - Core adapter functionality
- `Ecto.Adapter.Queryable` - Query execution and streaming
- `Ecto.Adapter.Schema` - Schema operations (insert, update, delete)

## Requirements

If using the provided `Snowflex.Transport.Http` transport, the only currently supported authentication method is keypair.

In order to obtain the `public_key_fingerprint`, please follow [Snowflake's instructions](https://docs.snowflake.com/en/user-guide/key-pair-auth#verify-the-user-s-public-key-fingerprint).

## Configuration

Configure the adapter in your application:

The adapter supports configurable transport implementations through the `:transport` option.
By default, it uses `Snowflex.Transport.Http` for REST API communication with Snowflake.

```elixir
config :my_app, MyApp.Repo,
  adapter: Snowflex,
  transport: Snowflex.Transport.Http,  # Optional, defaults to Http
  # Additional options passed to the transport
  account_name: "your-account",
  username: "your_username",
  private_key_path: "path/to/key.pem",
  public_key_fingerprint: "your_fingerprint"
```

You may supply other transports that conform to the `Snowflex.Transport` behaviour.

For additional configuration options of the provided `Snowflex.Transport.Http` transport, see it's documentation.

## Query Tagging

All queries can be tagged for better observability and tracking in Snowflake.
Tags are passed as options to any Repo function call:

```elixir
# Tag a query with a UUID
MyRepo.all(query, query_tag: Ecto.UUID.generate())

# Tag a query with a custom identifier
iex> MyRepo.insert(changeset, query_tag: "user_registration_abc")
```

Query tags are visible in Snowflake's query history and can be used for:

- Tracking query origins
- Monitoring specific operations
- Debugging performance issues
- Auditing database access

## Type Support

The adapter supports the following type conversions:

### From Snowflake to Ecto

- `:integer` - Integer values
- `:decimal` - Decimal values
- `:float` - Float values
- `:date` - Date values
- `:time` - Time values
- `:utc_datetime` - UTC datetime values
- `:naive_datetime` - Naive datetime values
- `:binary` - Binary data (as hex strings)
- `:map` - JSON/VARIANT data

### From Ecto to Snowflake

- `:binary` - Binary data (as hex strings)
- `:decimal` - Decimal values
- `:float` - Float values
- `:date` - Date values
- `:time` - Time values
- `:utc_datetime` - UTC datetime values
- `:naive_datetime` - Naive datetime values
- `:map` - JSON/VARIANT data

## Limitations/Considerations

### Transactions

Snowflex does not support multi-statement transactions. The reason for this is the [Snowflake SQL API](https://docs.snowflake.com/en/developer-guide/sql-api/submitting-multiple-statements) does not support multi-request transactions. That is to say, all statements in a transaction _must_ be sent in the same request. Because it is a common pattern to rely on the results of a previous statement in further downstream queries in the same transaction (e.g. `Ecto.Multi`), this limitation in the SQL API meant that we either needed to provide a potentially unintuitive use case, or just not support them at all.

### Multiple Statements

Snowflex supports submitting multiple statements in the same query and will return the results of each statement packed into an array.  

This can be useful when statements you want to execute need to occur inside of the same transaction (e.g. you need to leverage a temporary table)
This is possible using both the `query` and `query_many` functions.

``` elixir
iex> Repo.query("SELECT 1; SELECT 2;")

{:ok,
 [
   %Snowflex.Result{rows: [[1]]},
   %Snowflex.Result{rows: [[2]]}
 ]
}
```

### Streaming

When streaming rows using `Snowflex.Transport.Http`, keep in mind that [Snowflake dictates the number of partitions returned](https://docs.snowflake.com/en/developer-guide/sql-api/handling-responses#retrieving-additional-partitions). This is different than a normal TCP protocol like `Postgrex`, where the stream will be iterating on one row at a time.

Internally we utilize the same `Stream` modules as other implementations, but because each traunch of results is being determined externally to your app, that memory usage will be higher than if we were bringing back one row at a time.

### Migrations

Migrations are not currently supported by Snowflex.

## Migration from ODBC

Previous versions of Snowflex relied on Erlang `:odbc`. While very stable and battle tested, has always suffered from the idiosyncrasies inherent in ODBC, as well as limitations on the Snowflake side.

This V1.0 implementation removes support for ODBC, and instead relies solely on Snowflake's SQL API.

If you want to progressively migrate to the Ecto implementation, these tips might be helpful:

- Remove all `Snowflex.*_param()` wrapped functions.
- In your Snowflake Repo, add a declaration similar to the following:

```elixir
defmodule MyApp.Snowflake do
  use Ecto.Repo, otp_app: :my_app, adapter: Snowflex

  def sql_query(query) do
    execute(query)
  end

  def param_query(query, params \\ %{}) do
    execute(query, params)
  end

  defp execute(query, params \\ %{}) do
    query
    |> query_many!(params) # provided by `Ecto.Repo`
    |> process_results()
    |> unwrap_single_result()
  end

  defp process_results([]), do: []

   defp process_results([%Snowflex.Result{} = result | rest]),
     do: [unpack_snowflex_result(result) | process_results(rest)]
   defp process_results([other | rest]), do: [other | process_results(rest)]
   defp unpack_snowflex_result(%{columns: nil, num_rows: num_rows}), do: {:updated, num_rows}
   defp unpack_snowflex_result(%{columns: columns, rows: rows})
        when is_list(columns) and is_list(rows) do
     headers = Enum.map(columns, &(to_string(&1) |> String.downcase()))
     rows
     |> Enum.map(fn row ->
       Enum.zip(headers, row) |> Map.new()
     end)
   end
   # If there's just one result, unwrap it
   defp unwrap_single_result([result]), do: result
   defp unwrap_single_result(results), do: results
end
```

Any references to Snowflex for `sql_query` and `param_query` can then be replaced with `MyApp.Snowflake`.

To replace the previous functionality of `cast_results`, we would recommend that you leverage a schemaless changeset.

## Testing in your Project

When running tests locally, it can often be helpful to avoid hitting Snowflake to avoid unnecessary compute/storage costs.

See `Snowflex.MigrationGenerator` for a strategy to use a local DB implementation when running unit tests, while still using Snowflake in dev/prod environments.

<!-- MDOC -->

## Contributing

We provide a set of modules tagged as `:integration`.

If you'd like to run the integration tests, you will need to provide `Http` with appropriate configuration in order to connect.

See `test/support/schemas` for example schemas that you will need to make sure are available in your Snowflake environment.

To ensure a commit passes CI, please run `mix check`.

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
