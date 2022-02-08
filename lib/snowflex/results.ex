defmodule Snowflex.Results do
  def process(data, opts) when is_list(data) do
    Enum.map(data, &process(&1, opts))
  end

  def process({:selected, headers, rows}, opts) do
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

  def process({:updated, _} = results, _opts), do: results

  defp to_string_if_charlist(data) when is_list(data), do: to_string(data)
  defp to_string_if_charlist(data), do: data

  defp map_null_to_nil(:null, true), do: nil
  defp map_null_to_nil(data, _), do: data
end
