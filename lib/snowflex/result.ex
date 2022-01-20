defmodule Snowflex.Result do
  @moduledoc false

  defstruct action: nil,
            headers: nil,
            rows: nil,
            num_rows: 0,
            statement: nil,
            metadata: [],
            messages: [],
            success: false

  @type t :: %__MODULE__{
          action: :select | :update | nil,
          headers: [String.t()] | nil,
          rows: [tuple()] | nil,
          num_rows: integer(),
          statement: String.t() | nil,
          metadata: [map()],
          messages: [map()],
          success: boolean()
        }

  def from_update(statement, num_rows) do
    %__MODULE__{
      action: :update,
      num_rows: num_rows,
      statement: statement,
      success: true
    }
  end

  def from_headers_and_rows(statement, headers, rows) do
    %__MODULE__{
      action: :select,
      headers: normalize_headers(headers),
      rows: rows,
      num_rows: length(rows),
      statement: statement,
      success: true
    }
  end

  # Keep the process_result functions for backward compatibility
  def process_result(result, opts \\ [])

  def process_result(%__MODULE__{headers: headers, rows: rows}, opts) do
    process_results({:selected, headers, rows}, opts)
  end

  def process_result({:updated, _} = result, _opts), do: result

  def to_rows(%__MODULE__{headers: headers, rows: rows}, opts \\ []) do
    map_nulls_to_nil? = Keyword.get(opts, :map_nulls_to_nil?, true)

    for row <- rows do
      for {header, field} <- Enum.zip(headers, Tuple.to_list(row)), into: %{} do
        {header,
         field
         |> handle_encoding()
         |> to_string_if_charlist()
         |> map_null_to_nil(map_nulls_to_nil?)}
      end
    end
  end

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

  defp normalize_headers(headers) do
    Enum.map(headers, &(&1 |> to_string_if_charlist() |> String.downcase()))
  end

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
