defmodule Snowflex do
  @moduledoc """
  The client interface for connecting to the Snowflake data warehouse. This module should not be used directly except for the types. The preferred method is to use a `Snowflex.Connection` to manage all query executions.

  The main entry point to this module is `Snowflex.do_query`. This function takes a `Snowflex.Query` struct containing
  a SQL query and returns a list of maps (one per row). NOTE: due to the way the Erlang ODBC works, all values comeback
  as strings. You will need to cast values appropriately.
  """
  alias Ecto.Changeset
  alias Snowflex.{Worker, Query}

  # Shamelessly copied from http://erlang.org/doc/man/odbc.html#common-data-types-
  @type precision :: integer()
  @type scale :: integer()
  @type size :: integer()
  @type odbc_data_type ::
          :sql_integer
          | :sql_smallint
          | :sql_tinyint
          | {:sql_decimal, precision(), scale()}
          | {:sql_numeric, precision(), scale()}
          | {:sql_char, size()}
          | {:sql_wchar, size()}
          | {:sql_varchar, size()}
          | {:sql_wvarchar, size()}
          | {:sql_float, precision()}
          | {:sql_wlongvarchar, size()}
          | {:sql_float, precision()}
          | :sql_real
          | :sql_double
          | :sql_bit
          | atom()
  @type value :: nil | term()

  @type query_param :: {odbc_data_type(), [value()]}
  @type sql_data :: list(%{optional(String.t()) => String.t()})
  @type connection_opts :: [timeout: timeout(), map_null_to_nil?: boolean()]

  @spec do_query(pool_name :: atom(), query :: Query.t(), connection_opts()) ::
          sql_data() | {:error, term()}
  def do_query(pool_name, query = %Query{params: nil}, opts) do
    timeout = Keyword.get(opts, :timeout)

    case :poolboy.transaction(
           pool_name,
           &Worker.sql_query(&1, query.query_string, timeout),
           timeout
         ) do
      {:ok, results} -> process_results(results, opts)
      err -> err
    end
  end

  def do_query(pool_name, query = %Query{}, opts) do
    timeout = Keyword.get(opts, :timeout)

    case :poolboy.transaction(
           pool_name,
           &Worker.param_query(&1, query.query_string, query.params, timeout),
           timeout
         ) do
      {:ok, results} -> process_results(results, opts)
      err -> err
    end
  end

  @doc """
  Cast data results from a query into a given schema
  """
  @spec cast_results(data :: Enum.t(), schema :: Ecto.Schema.t()) :: list
  def cast_results(data, schema) do
    Enum.map(data, &cast_row(&1, schema))
  end

  # Helpers

  defp process_results(data, opts) when is_list(data) do
    Enum.map(data, &process_results(&1, opts))
  end

  defp process_results({:selected, headers, rows}, opts) do
    map_nulls_to_nil? = Keyword.get(opts, :map_nulls_to_nil?)

    bin_headers =
      headers
      |> Enum.map(fn header -> header |> to_string() |> String.downcase() end)
      |> Enum.with_index()

    Enum.map(rows, fn row ->
      Enum.reduce(bin_headers, %{}, fn {col, index}, map ->
        data =
          row
          |> elem(index)
          |> to_string_if_charlist()
          |> map_null_to_nil(map_nulls_to_nil?)

        Map.put(map, col, data)
      end)
    end)
  end

  defp to_string_if_charlist(data) when is_list(data), do: to_string(data)
  defp to_string_if_charlist(data), do: data

  defp map_null_to_nil(:null, true), do: nil
  defp map_null_to_nil(data, _), do: data

  defp cast_row(row, schema) do
    schema
    |> struct()
    |> Changeset.cast(row, schema.__schema__(:fields))
    |> Changeset.apply_changes()
  end
end
