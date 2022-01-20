defmodule Snowflex.Transport.ODBC do
  @moduledoc false

  alias Snowflex.Result

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
  @type param :: {odbc_data_type(), [value()]}

  @behaviour Snowflex.Transport

  @impl true
  def connect(opts) do
    {connection_args, opts} = Keyword.pop!(opts, :connection)
    conn_str = connection_args |> connection_string() |> to_charlist()
    :odbc.connect(conn_str, opts)
  end

  @impl true
  def disconnect(conn), do: :odbc.disconnect(conn)

  @impl true
  def sql_query(conn, query) do
    conn
    |> :odbc.sql_query(to_charlist(query))
    |> handle_response(query)
  end

  @impl true
  def param_query(conn, query, params) do
    conn
    |> :odbc.param_query(to_charlist(query), params)
    |> handle_response(query)
  end

  # Parameter Helpers

  @doc "Construct an integer parameter."
  @spec int_param(integer()) :: param()
  def int_param(val), do: {:sql_integer, val}

  @doc "Construct a string parameter."
  @spec string_param(String.t(), non_neg_integer()) :: param()
  def string_param(val, length \\ 250), do: {{:sql_varchar, length}, val}

  # ---

  defp handle_response({:error, reason}, _), do: {:error, reason}

  defp handle_response({:updated, num_rows}, query) do
    {:ok, Result.from_update(query, num_rows)}
  end

  defp handle_response({:selected, headers, rows}, query) do
    {:ok, Result.from_headers_and_rows(query, headers, rows)}
  end

  defp handle_response(results, query) when is_list(results) do
    {:ok,
     results
     |> Enum.map(&handle_response(&1, query))
     |> Enum.map(fn {:ok, res} -> res end)}
  end

  defp connection_string(connection_args) do
    driver = Application.get_env(:snowflex, :driver)
    connection_args = [{:driver, driver} | connection_args]
    Enum.map_join(connection_args, ";", fn {k, v} -> "#{k}=#{v}" end)
  end
end
