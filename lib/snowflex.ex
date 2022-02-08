defmodule Snowflex do
  @moduledoc """
  The client interface for connecting to the Snowflake data warehouse.

  The main entry point to this module is `Snowflex.sql_query`. This function takes a string containing
  a SQL query and returns a list of maps (one per row). NOTE: due to the way the Erlang ODBC works, all values comeback
  as strings. You will need to cast values appropriately.
  """
  alias Snowflex.Worker
  alias Snowflex.Results
  alias Ecto.Changeset

  # Shamelessly copied from http://erlang.org/doc/man/odbc.html#common-data-types-
  @type precision :: integer()
  @type scale :: integer()
  @type size :: integer()
  @type odbc_data_type ::
          :sql_integer
          | :sql_smallint
          | :sql_tinyint
          | {:sql_decimal, precision(), scale()}
          | {:sql_numeric, precision(), scale()}
          | {:sql_char, size()}
          | {:sql_wchar, size()}
          | {:sql_varchar, size()}
          | {:sql_wvarchar, size()}
          | {:sql_float, precision()}
          | {:sql_wlongvarchar, size()}
          | {:sql_float, precision()}
          | :sql_real
          | :sql_double
          | :sql_bit
          | atom()
  @type value :: nil | term()

  @type query_param :: {odbc_data_type(), [value()]}
  @type sql_data :: list(%{optional(String.t()) => String.t()})
  @type query_opts :: [timeout: timeout(), map_nulls_to_nil?: boolean()]

  @spec sql_query(atom(), String.t(), query_opts()) ::
          sql_data() | {:error, term()} | {:updated, integer()}
  def sql_query(pool_name, query, opts) do
    timeout = Keyword.get(opts, :timeout)

    case :poolboy.transaction(
           pool_name,
           &Worker.sql_query(&1, query, timeout),
           timeout
         ) do
      {:ok, results} -> Results.process(results, opts)
      err -> err
    end
  end

  @spec param_query(atom(), String.t(), list(query_param()), query_opts()) ::
          sql_data() | {:error, term()} | {:updated, integer()}
  def param_query(pool_name, query, params, opts) do
    timeout = Keyword.get(opts, :timeout)

    case :poolboy.transaction(
           pool_name,
           &Worker.param_query(&1, query, params, timeout),
           timeout
         ) do
      {:ok, results} -> Results.process(results, opts)
      err -> err
    end
  end

  @spec stream(atom(), String.t(), query_opts()) :: Stream.t()
  def stream(pool_name, query, opts) do
    timeout = Keyword.get(opts, :timeout)

    case :poolboy.transaction(
           pool_name,
           &Worker.stream(&1, query, timeout),
           timeout
         ) do
      {:ok, stream} -> stream
      err -> err
    end
  end

  @spec stream(atom(), String.t(), function(), query_opts()) :: Stream.t()
  def stream(pool_name, query, fun, opts) do
    timeout = Keyword.get(opts, :timeout)

    pid = :poolboy.checkout(pool_name)

    case Worker.stream(pid, query, fun, timeout) do
      {:ok, stream} -> stream
      err -> err
    end
  end

  def cast_results(data, schema) do
    Enum.map(data, &cast_row(&1, schema))
  end

  defp cast_row(row, schema) do
    schema
    |> struct()
    |> Changeset.cast(row, schema.__schema__(:fields))
    |> Changeset.apply_changes()
  end

  def int_param(val), do: {:sql_integer, val}
  def string_param(val, length \\ 250), do: {{:sql_varchar, length}, val}
end
