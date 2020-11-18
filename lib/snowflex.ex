defmodule Snowflex do
  @moduledoc """
  The client interface for connecting to the Snowflake data warehouse.

  The main entry point to this module is `Snowflex.sql_query`. This function takes a string containing
  a SQL query and returns a list of maps (one per row). NOTE: due to the way the Erlang ODBC works, all values comeback
  as strings. You will need to cast values appropriately.
  """
  alias Ecto.Changeset
  alias Snowflex.Worker

  @timeout :timer.seconds(60)
  @type query_param :: {:odbc.odbc_data_type(), list(:odbc.value())}
  @type sql_data :: list(%{optional(String.t()) => String.t()})

  @spec sql_query(atom(), String.t(), non_neg_integer() | :infinity) ::
          sql_data() | {:error, term}
  def sql_query(pool_name, query, timeout \\ @timeout) do
    case :poolboy.transaction(
           pool_name,
           fn pid -> Worker.sql_query(pid, query, timeout) end,
           timeout
         ) do
      {:ok, results} -> process_results(results)
      err -> err
    end
  end

  @spec param_query(atom(), String.t(), list(query_param()), non_neg_integer() | :infinity) ::
          sql_data() | {:error, term}
  def param_query(pool_name, query, params \\ [], timeout \\ @timeout) do
    case :poolboy.transaction(
           pool_name,
           fn pid -> Worker.param_query(pid, query, params, timeout) end,
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

  defp to_string_if_charlist(data) when is_list(data), do: to_string(data)
  defp to_string_if_charlist(data), do: data

  defp cast_row(row, schema) do
    schema
    |> struct()
    |> Changeset.cast(row, schema.__schema__(:fields))
    |> Changeset.apply_changes()
  end
end
