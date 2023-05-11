defmodule Snowflex.Result do
  defstruct columns: nil,
            rows: nil,
            num_rows: 0,
            metadata: [],
            messages: [],
            statement: nil,
            success: false

  @type t :: %__MODULE__{
          columns: [String.t()] | nil,
          rows: [tuple()] | nil,
          num_rows: integer(),
          metadata: [map()],
          messages: [map()],
          statement: String.t() | nil,
          success: boolean()
        }

  def process_result(result, opts \\ [])

  def process_result(%__MODULE__{columns: columns, rows: rows}, opts) do
    process_results({:selected, columns, rows}, opts)
  end

  def process_result({:updated, _} = result, _opts), do: result

  ## Helpers

  defp process_results(data, opts) when is_list(data) do
    Enum.map(data, &process_results(&1, opts))
  end

  defp process_results({:selected, headers, rows}, opts) do
    map_nulls_to_nil? = Keyword.get(opts, :map_nulls_to_nil?, true)

    bin_headers =
      headers
      |> Enum.map(fn header -> header |> to_string() |> String.downcase() end)
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

  defp process_results({:updated, _} = results, _opts), do: results

  defp to_string_if_charlist(data) when is_list(data), do: to_string(data)
  defp to_string_if_charlist(data), do: data

  defp handle_encoding(data) when is_list(data) do
    raw = :erlang.list_to_binary(data)

    case :unicode.characters_to_binary(raw) do
      utf8 when is_binary(utf8) -> utf8
      _ -> :unicode.characters_to_binary(raw, :latin1)
    end
  end

  defp handle_encoding(data), do: data

  defp map_null_to_nil(:null, true), do: nil
  defp map_null_to_nil(data, _), do: data
end
