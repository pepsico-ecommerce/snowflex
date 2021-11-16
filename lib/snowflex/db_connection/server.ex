defmodule Snowflex.DBConnection.Server do
  @moduledoc """
  Adapter to Erlang's `:odbc` module.

  A GenServer that handles communication between Elixir and Erlang's `:odbc` module.
  """

  use GenServer

  require Logger

  alias Snowflex.DBConnection.Error

  @timeout :timer.seconds(60)
  @begin_transaction 'begin transaction;'
  @last_query_id 'SELECT LAST_QUERY_ID() as query_id;'
  @close_transaction 'commit;'

  ## Public API

  @doc """
  Starts the connection process to the ODBC driver.
  """
  @spec start_link(binary(), Keyword.t()) :: {:ok, pid()}
  def start_link(conn_str, opts) do
    GenServer.start_link(__MODULE__, [{:conn_str, to_charlist(conn_str)} | opts])
  end

  @doc """
  Sends a parametrized query to the ODBC driver.

  `pid` is the `:odbc` process id
  `statement` is the SQL query string
  `params` are the parameters to send with the SQL query
  `opts` are options to be passed on to `:odbc`
  `with_query_id?` runs query in transaction and selects LAST_QUERY_ID()
  """
  @spec query(pid(), iodata(), Keyword.t(), Keyword.t(), boolean()) ::
          {:selected, [binary()], [tuple()]}
          | {:selected, [binary()], [tuple()], [{binary()}]}
          | {:updated, non_neg_integer()}
          | {:error, Error.t()}
  def query(pid, statement, params, opts, with_query_id? \\ false) do
    # TODO add telemetry
    if Process.alive?(pid) do
      statement = IO.iodata_to_binary(statement)
      timeout = Keyword.get(opts, :timeout, @timeout)

      GenServer.call(
        pid,
        {:query, %{statement: statement, params: params, with_query_id: with_query_id?}},
        timeout
      )
    else
      {:error, %Error{message: :no_connection}}
    end
  end

  @doc """
  Disconnects from the ODBC driver.
  """
  @spec disconnect(pid()) :: :ok
  def disconnect(pid) do
    GenServer.stop(pid, :normal)
  end

  ## GenServer callbacks

  @impl GenServer
  def init(opts) do
    send(self(), {:start, opts})

    {:ok, %{backoff: :backoff.init(2, 60), state: :not_connected}}
  end

  @impl GenServer
  def handle_call({:query, _query}, _from, %{state: :not_connected} = state) do
    {:reply, {:error, :not_connected}, state}
  end

  def handle_call(
        {:query, %{statement: statement, params: params, with_query_id: false}},
        _from,
        %{pid: pid} = state
      ) do
    case :odbc.param_query(pid, to_charlist(statement), params) do
      {:error, reason} ->
        error = Error.exception(reason)
        Logger.warn("Unable to execute query: #{error.message}")

        {:reply, {:error, error}, state}

      result ->
        {:reply, result, state}
    end
  end

  def handle_call(
        {:query, %{statement: statement, params: params, with_query_id: true}},
        _from,
        %{pid: pid} = state
      ) do
    :odbc.sql_query(pid, @begin_transaction)

    case :odbc.param_query(pid, to_charlist(statement), params) do
      {:error, reason} ->
        error = Error.exception(reason)
        Logger.warn("Unable to execute query: #{error.message}")

        :odbc.sql_query(pid, @close_transaction)

        {:reply, {:error, error}, state}

      result ->
        {:selected, _, query_id} = :odbc.sql_query(pid, @last_query_id)

        :odbc.sql_query(pid, @close_transaction)

        {:reply, Tuple.append(result, query_id), state}
    end
  end

  @impl GenServer
  def handle_info({:start, opts}, %{backoff: backoff} = _state) do
    connect_opts =
      opts
      |> Keyword.delete_first(:conn_str)
      |> Keyword.put_new(:auto_commit, :on)
      |> Keyword.put_new(:binary_strings, :on)
      |> Keyword.put_new(:tuple_row, :on)
      |> Keyword.put_new(:extended_errors, :on)

    case :odbc.connect(opts[:conn_str], connect_opts) do
      {:ok, pid} ->
        {:noreply, %{pid: pid, backoff: :backoff.succeed(backoff), state: :connected}}

      {:error, reason} ->
        Logger.warn("Unable to connect to snowflake: #{inspect(reason)}")

        seconds =
          backoff
          |> :backoff.get()
          |> :timer.seconds()

        Process.send_after(self(), {:start, opts}, seconds)

        {_, new_backoff} = :backoff.fail(backoff)

        {:noreply, %{backoff: new_backoff, state: :not_connected}}
    end
  end

  @impl GenServer
  def terminate(_reason, %{state: :not_connected} = _state), do: :ok
  def terminate(_reason, %{pid: pid} = _state), do: :odbc.disconnect(pid)
end
