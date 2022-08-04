defmodule Snowflex.Query do
  defstruct [
    :ref,
    :name,
    :statement,
    :columns,
    :result_oids,
    cache: :reference
  ]

  defimpl DBConnection.Query do
    def parse(query, _opts), do: query
    def describe(query, _opts), do: query
    def encode(_query, params, _opts), do: params
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
