defmodule Snowflex do
  @moduledoc """
  The client interface for connecting to the Snowflake data warehouse.

  The main entry point to this module is `Snowflex.sql_query/3`. This function takes
  a string containing a SQL query and returns a list of maps (one per row).

  NOTE: due to the way the Erlang ODBC works, all values come back as strings when
  using the default `:odbc` transport. You will need to cast values appropriately
  (you may use the provided `Snowflex.cast_results/2` to cast to Ecto structs).
  """
  alias Ecto.Changeset
  alias Snowflex.{Worker, Transport, Result, Query, Connection}

  @type sql_data :: list(%{optional(String.t()) => String.t()})
  @type query_opts :: [timeout: timeout(), map_nulls_to_nil?: boolean()]

  defmacrop is_iodata(data) do
    quote do
      is_list(unquote(data)) or is_binary(unquote(data))
    end
  end

  @transport (case Application.compile_env(:snowflex, :transport, :odbc) do
                :odbc ->
                  Transport.ODBC

                :http ->
                  Transport.HTTP

                transport ->
                  raise "unrecognized transport #{inspect(transport)} configured for :snowflex"
              end)

  @doc false
  @spec transport :: Transport.ODBC | Transport.HTTP
  def transport, do: @transport

  def child_spec(options) do
    DBConnection.child_spec(Connection, options)
  end

  @doc "Peform an SQL query."
  @spec sql_query(atom(), String.t(), query_opts()) ::
          sql_data() | {:error, term()} | {:updated, integer()}
  def sql_query(pool_name, query, opts) do
    timeout = Keyword.get(opts, :timeout)
    callback = &Worker.sql_query(&1, query, timeout)

    pool_name
    |> :poolboy.transaction(callback, timeout)
    |> handle_response(opts)
  end

  @doc "Peform a parametrized SQL query."
  @spec param_query(atom(), String.t(), list(Transport.param()), query_opts()) ::
          sql_data() | {:error, term()} | {:updated, integer()}
  def param_query(pool_name, query, params, opts) do
    timeout = Keyword.get(opts, :timeout)
    callback = &Worker.param_query(&1, query, params, timeout)

    pool_name
    |> :poolboy.transaction(callback, timeout)
    |> handle_response(opts)
  end

  def prepare_execute(conn, name, statement, params \\ [], opts \\ [])
      when is_iodata(name) and is_iodata(statement) do
    query = %Query{name: name, statement: statement}
    DBConnection.prepare_execute(conn, query, params, opts)
  end

  def query(conn, statement, params \\ [], options \\ []) when is_iodata(statement) do
    name = options[:cache_statement]
    query_type = options[:query_type] || :binary

    cond do
      name != nil ->
        statement = IO.iodata_to_binary(statement)
        query = %Query{name: name, statement: statement, cache: :statement}
        do_query(conn, query, params, options)

      query_type in [:binary, :binary_then_text] ->
        query = %Query{name: "", statement: statement}
        do_query(conn, query, params, options)
    end
  end

  def execute(conn, %Query{} = query, params \\ [], opts \\ []) do
    DBConnection.execute(conn, query, params, opts)
  end

  defp do_query(conn, %Query{} = query, params, options) do
    conn
    |> DBConnection.prepare_execute(query, params, options)
    |> query_result()
  end

  defp query_result({:ok, _query, result}), do: {:ok, result}
  defp query_result({:error, _} = error), do: error

  # Legacy parameter helpers with deprecation notices
  @deprecated "Use transport-specific helper instead (see `t:Snowflex.Transport.param/0`)."
  @spec int_param(integer()) :: Transport.param()
  defdelegate int_param(val), to: Transport.ODBC

  @deprecated "Use transport-specific helper instead (see `t:Snowflex.Transport.param/0`)."
  @spec string_param(String.t(), non_neg_integer()) :: Transport.param()
  defdelegate string_param(val, length \\ 250), to: Transport.ODBC

  @deprecated "Use transport-specific helper instead (see `t:Snowflex.Transport.param/0`)."
  def unicode_string_param(value) do
    case :unicode.characters_to_binary(value, :unicode, {:utf16, :little}) do
      utf16 when is_bitstring(utf16) ->
        {{:sql_wvarchar, byte_size(value)}, [utf16]}

      _ ->
        raise "Snowflex failed to convert string to UTF16LE: #{value}"
    end
  end

  defp handle_response({:ok, %Result{action: :update, num_rows: num_rows}}, _) do
    {:updated, num_rows}
  end

  defp handle_response({:ok, %Result{action: :select} = result}, opts) do
    Result.to_rows(result, opts)
  end

  defp handle_response({:error, reason}, _), do: {:error, reason}

  if Code.ensure_loaded?(Ecto) do
    @doc """
    Cast all result rows from a query into the given Ecto `schema` module.

    This function is only available when the `:ecto` dependency is included.
    """
    @spec cast_results([map()], module()) :: [Ecto.Changeset.t()]
    def cast_results(rows, schema) do
      Enum.map(rows, &cast_row(&1, schema))
    end

    defp cast_row(row, schema) do
      schema
      |> struct()
      |> Changeset.cast(row, schema.__schema__(:fields))
      |> Changeset.apply_changes()
    end
  end
end
