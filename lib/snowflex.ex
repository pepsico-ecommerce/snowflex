defmodule Snowflex do
  @moduledoc """
  The client interface for connecting to the Snowflake data warehouse.

  The main entry point to this module is `Snowflex.sql_query`. This function takes a string containing
  a SQL query and returns a list of maps (one per row). NOTE: due to the way the Erlang ODBC works, all values comeback
  as strings. You will need to cast values appropriately.
  """
  alias Ecto.Changeset
  alias Snowflex.Worker

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

  @spec sql_query(atom(), String.t(), timeout()) ::
          sql_data() | {:error, term()}
  def sql_query(pool_name, query, timeout) do
    case :poolboy.transaction(
           pool_name,
           &Worker.sql_query(&1, query, timeout),
           timeout
         ) do
      {:ok, results} -> process_results(results)
      err -> err
    end
  end

  @spec param_query(atom(), String.t(), list(query_param()), timeout()) ::
          sql_data() | {:error, term()}
  def param_query(pool_name, query, params \\ [], timeout) do
    case :poolboy.transaction(
           pool_name,
           &Worker.param_query(&1, query, params, timeout),
           timeout
         ) do
      {:ok, results} -> process_results(results)
      err -> err
    end
  end

  def cast_results(data, schema) do
    Enum.map(data, &cast_row(&1, schema))
  end

  def int_param(val), do: {:sql_integer, val}
  def string_param(val, length \\ 250), do: {{:sql_varchar, length}, val}

  # Helpers

  defp process_results(data) when is_list(data) do
    Enum.map(data, &process_results(&1))
  end

  defp process_results({:selected, headers, rows}) do
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

        Map.put(map, col, data)
      end)
    end)
  end
  defp process_results(results), do: results

  defp to_string_if_charlist(data) when is_list(data), do: to_string(data)
  defp to_string_if_charlist(data), do: data

  defp cast_row(row, schema) do
    schema
    |> struct()
    |> Changeset.cast(row, schema.__schema__(:fields))
    |> Changeset.apply_changes()
  end
end
