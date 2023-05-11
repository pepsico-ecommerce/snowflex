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
  @type query_opts :: [timeout: timeout(), map_nulls_to_nil?: boolean()]

  @spec sql_query(atom(), String.t(), query_opts()) ::
          sql_data() | {:error, term()} | {:updated, integer()}
  def sql_query(pool_name, query, opts) do
    timeout = Keyword.get(opts, :timeout)

    case :poolboy.transaction(
           pool_name,
           &Worker.sql_query(&1, query, timeout),
           timeout
         ) do
      {:ok, results} -> process_results(results, opts)
      err -> err
    end
  end

  @spec param_query(atom(), String.t(), list(query_param()), query_opts()) ::
          sql_data() | {:error, term()} | {:updated, integer()}
  def param_query(pool_name, query, params, opts) do
    timeout = Keyword.get(opts, :timeout)

    case :poolboy.transaction(
           pool_name,
           &Worker.param_query(&1, query, params, timeout),
           timeout
         ) do
      {:ok, results} -> process_results(results, opts)
      err -> err
    end
  end

  def cast_results(data, schema) do
    Enum.map(data, &cast_row(&1, schema))
  end

  def int_param(val), do: {:sql_integer, val}
  def string_param(val, length \\ 250), do: {{:sql_varchar, length}, val}

  def unicode_string_param(value) do
    case :unicode.characters_to_binary(value, :unicode, {:utf16, :little}) do
      utf16 when is_bitstring(utf16) ->
        {{:sql_wvarchar, byte_size(value)}, [utf16]}

      _ ->
        raise "Snowflex failed to convert string to UTF16LE: #{value}"
    end
  end

  # Helpers

  defp process_results(data, opts) when is_list(data) do
    Enum.map(data, &process_results(&1, opts))
  end

  defp process_results({:selected, headers, rows}, opts) do
    map_nulls_to_nil? = Keyword.get(opts, :map_nulls_to_nil?)

    bin_headers =
      headers
      |> Enum.map(fn header ->
        header
        |> to_string()
        |> then(fn name ->
          case opts[:keep_column_case?] do
            true -> name
            false -> String.downcase(name)
          end
        end)
      end)
      |> Enum.with_index()

    Enum.map(rows, fn row ->
      Enum.reduce(bin_headers, %{}, fn {col, index}, map ->
        data =
          row
          |> elem(index)
          |> handle_encoding()
          |> to_string_if_charlist()
          |> map_null_to_nil(map_nulls_to_nil?)

        Map.put(map, col, data)
      end)
    end)
  end

  defp process_results(results), do: results

  defp process_results({:updated, _} = results, _opts), do: results

  defp to_string_if_charlist(data) when is_list(data), do: to_string(data)
  defp to_string_if_charlist(data), do: data

  defp map_null_to_nil(:null, true), do: nil
  defp map_null_to_nil(data, _), do: data

  defp handle_encoding(data) when is_list(data) do
    raw = :erlang.list_to_binary(data)

    case :unicode.characters_to_binary(raw) do
      utf8 when is_binary(utf8) -> utf8
      _ -> :unicode.characters_to_binary(raw, :latin1)
    end
  end

  defp handle_encoding(data), do: data

  defp cast_row(row, schema) do
    schema
    |> struct()
    |> Changeset.cast(row, schema.__schema__(:fields))
    |> Changeset.apply_changes()
  end
end
