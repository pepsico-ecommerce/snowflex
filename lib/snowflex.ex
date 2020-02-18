defmodule Snowflex do
  alias Snowflex.Worker

  @moduledoc """
  The client interface for connecting to the Snowflake data warehouse.

  The main entry point to this module is `Snowflex.sql_query`. This function takes a string containing
  a SQL query and returns a list of maps (one per row). NOTE: due to the way the Erlang ODBC works, all values comeback
  as strings. You will need to cast values appropriately.
  """

  @timeout :timer.seconds(60)
  @type query_param :: {:odbc.odbc_data_type(), list(:odbc.value())}
  @type sql_data :: list(%{optional(String.t()) => String.t()})

  @spec sql_query(String.t()) :: sql_data()
  def sql_query(query) do
    case :poolboy.transaction(
           :snowflake_pool,
           fn pid -> Worker.sql_query(pid, query) end,
           @timeout
         ) do
      {:ok, results} -> process_results(results)
      err -> err
    end
  end

  @spec param_query(String.t(), list(query_param())) :: sql_data()
  def param_query(query, params \\ []) do
    case :poolboy.transaction(
           :snowflake_pool,
           fn pid -> Worker.param_query(pid, query, params) end,
           @timeout
         ) do
      {:ok, results} -> process_results(results)
      err -> err
    end
  end

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
end
