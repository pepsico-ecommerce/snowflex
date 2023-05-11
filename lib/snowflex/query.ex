defmodule Snowflex.Query do
  @moduledoc """
  Implementation of `DBConnection.Query` for `Snowflex`.
  """

  defstruct [
    :ref,
    :name,
    :statement,
    :columns,
    :result_oids,
    cache: :reference
  ]

  defimpl DBConnection.Query do
    alias Snowflex.Result
    alias Snowflex.Type

    def parse(query, _opts), do: query
    def describe(query, _opts), do: query

    @spec encode(query :: Query.t(), params :: [Type.param()], opts :: Keyword.t()) :: [
            Type.param()
          ]
    def encode(_query, params, opts) do
      Enum.map(params, &Type.encode(&1, opts))
    end

    @spec decode(query :: Query.t(), result :: Result.t(), opts :: Keyword.t()) :: Result.t()
    def decode(_query, %Result{rows: rows} = result, opts) when not is_nil(rows) do
      rows = Enum.map(rows, fn row -> Enum.map(row, &Type.decode(&1, opts)) end)
      Map.put(result, :rows, rows)
    end

    def decode(_query, result, _opts), do: result
  end

  defimpl String.Chars do
    alias Snowflex.Query

    def to_string(%{statement: statement}) do
      case statement do
        statement when is_binary(statement) -> IO.iodata_to_binary(statement)
        statement when is_list(statement) -> IO.iodata_to_binary(statement)
        %{statement: %Query{} = q} -> String.Chars.to_string(q)
      end
    end
  end
end
