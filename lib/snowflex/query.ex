defmodule Snowflex.Query do
  @moduledoc """
  Snowflake implementation of `DBConnection.Query`.
  """

  alias String.Chars

  defstruct [
    :statement,
    :transport,
    name: "",
    cache: :reference
  ]

  defguard is_statement(statement) when is_list(statement) or is_binary(statement)

  @doc false
  @spec new(Keyword.t()) :: t()
  def new(attrs) do
    struct(
      __MODULE__,
      Keyword.update!(attrs, :statement, &IO.iodata_to_binary/1)
    )
  end

  @type t :: %__MODULE__{}

  defimpl DBConnection.Query do
    alias Snowflex.Query
    alias Snowflex.Result
    alias Snowflex.Transport.Http.Type

    @spec parse(Query.t(), Keyword.t()) :: Query.t()
    def parse(query, _opts), do: query

    @spec describe(Query.t(), Keyword.t()) :: Query.t()
    def describe(query, _opts), do: query

    @spec encode(
            query :: Query.t(),
            params :: [Type.encodeable()],
            opts :: Keyword.t()
          ) :: [Type.encoded_value()]
    def encode(_query, params, opts) do
      Enum.map(params, &Type.encode(&1, opts))
    end

    @spec decode(query :: Query.t(), result :: Result.t(), opts :: Keyword.t()) :: Result.t()
    def decode(_query, %Result{rows: rows, columns: columns, metadata: metadata} = result, _opts)
        when is_list(rows) do
      column_types = extract_column_types(metadata)

      decoded_rows =
        for row <- rows do
          for {value, column_name} <- Enum.zip(row, columns) do
            column_type = get_column_type(column_types, column_name)
            Type.decode(value, %{column: column_name, type: column_type})
          end
        end

      Map.put(result, :rows, decoded_rows)
    end

    def decode(_query, result, _opts), do: result

    # Extract column type information from metadata
    defp extract_column_types(%{"rowType" => row_type}) when is_list(row_type) do
      Map.new(row_type, fn column_info -> {String.upcase(column_info["name"]), column_info} end)
    end

    defp extract_column_types(_), do: %{}

    # Get type information for a specific column
    defp get_column_type(column_types, column_name) when is_binary(column_name) do
      Map.get(column_types, String.upcase(column_name), %{})
    end

    defp get_column_type(_, _), do: %{}
  end

  defimpl String.Chars do
    alias Snowflex.Query

    @spec to_string(Query.t()) :: binary()
    def to_string(%{statement: statement}) do
      case statement do
        statement when is_binary(statement) -> IO.iodata_to_binary(statement)
        statement when is_list(statement) -> IO.iodata_to_binary(statement)
        %{statement: %Query{} = q} -> Chars.to_string(q)
      end
    end
  end
end
