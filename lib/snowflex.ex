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
  alias Snowflex.{Worker, Transport, Result}

  @type sql_data :: list(%{optional(String.t()) => String.t()})
  @type query_opts :: [timeout: timeout(), map_nulls_to_nil?: boolean()]

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

  @deprecated "Use transport-specific helper instead (see `t:Snowflex.Transport.param/0`)."
  @spec int_param(integer()) :: Transport.param()
  defdelegate int_param(val), to: Snowflex.Transport.ODBC

  @deprecated "Use transport-specific helper instead (see `t:Snowflex.Transport.param/0`)."
  @spec string_param(String.t(), non_neg_integer()) :: Transport.param()
  defdelegate string_param(val, length \\ 250), to: Snowflex.Transport.ODBC

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
