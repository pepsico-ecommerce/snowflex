defmodule Snowflex do
  defmacrop is_iodata(data) do
    quote do
      is_list(unquote(data)) or is_binary(unquote(data))
    end
  end

  def child_spec(options) do
    DBConnection.child_spec(Snowflex.Connection, options)
  end

  def prepare_execute(conn, name, statement, params \\ [], opts \\ [])
      when is_iodata(name) and is_iodata(statement) do
    query = %Snowflex.Query{name: name, statement: statement}
    DBConnection.prepare_execute(conn, query, params, opts)
  end

  def query(conn, statement, params \\ [], options \\ []) when is_iodata(statement) do
    name = options[:cache_statement]
    query_type = options[:query_type] || :binary

    cond do
      name != nil ->
        statement = IO.iodata_to_binary(statement)
        query = %Snowflex.Query{name: name, statement: statement, cache: :statement}
        do_query(conn, query, params, options)

      query_type in [:binary, :binary_then_text] ->
        query = %Snowflex.Query{name: "", statement: statement}
        do_query(conn, query, params, options)
    end
  end

  # @spec execute(conn(), Snowflex.Query.t(), list(), [option()]) ::
  # {:ok, MyXQL.Query.t(), MyXQL.Result.t()} | {:error, Exception.t()}
  def execute(conn, %Snowflex.Query{} = query, params \\ [], opts \\ []) do
    DBConnection.execute(conn, query, params, opts)
  end

  defp do_query(conn, %Snowflex.Query{} = query, params, options) do
    conn
    |> DBConnection.prepare_execute(query, params, options)
    |> query_result()
  end

  defp query_result({:ok, _query, result}), do: {:ok, result}
  defp query_result({:error, _} = error), do: error
end
