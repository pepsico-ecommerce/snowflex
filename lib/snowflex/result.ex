defmodule Snowflex.Result do
  @moduledoc false

  defstruct action: nil,
            headers: nil,
            rows: nil,
            num_rows: 0,
            statement: nil

  @type t :: %__MODULE__{
          action: :select | :update | nil,
          headers: [String.t()] | nil,
          rows: [tuple()] | nil,
          num_rows: integer(),
          statement: String.t() | nil
        }

  def from_update(statement, num_rows) do
    %__MODULE__{
      action: :update,
      num_rows: num_rows,
      statement: statement
    }
  end

  def from_headers_and_rows(statement, headers, rows) do
    %__MODULE__{
      action: :select,
      headers: normalize_headers(headers),
      rows: rows,
      num_rows: length(rows),
      statement: statement
    }
  end

  def to_rows(%__MODULE__{headers: headers, rows: rows}, opts \\ []) do
    map_nulls_to_nil? = Keyword.get(opts, :map_nulls_to_nil?, true)

    for row <- rows do
      for {header, field} <- Enum.zip(headers, Tuple.to_list(row)), into: %{} do
        {header,
         field
         |> to_string_if_charlist()
         |> map_null_to_nil(map_nulls_to_nil?)}
      end
    end
  end

  defp normalize_headers(headers) do
    Enum.map(headers, &(&1 |> to_string_if_charlist() |> String.downcase()))
  end

  defp to_string_if_charlist(data) when is_list(data), do: to_string(data)
  defp to_string_if_charlist(data), do: data

  defp map_null_to_nil(:null, true), do: nil
  defp map_null_to_nil(data, _), do: data
end
