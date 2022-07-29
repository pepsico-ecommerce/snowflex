defmodule Snowflex.Client do
  @moduledoc """
  Adapter to Erlang's `:odbc` module.

  A GenServer that handles communication between Elixir and Erlang's `:odbc` module.
  """

  use GenServer

  require Logger

  alias Snowflex.EctoAdapter.Error

  @timeout :timer.seconds(60)

  ## Public API

  @doc """
  Starts the connection process to the ODBC driver.
  """
  @spec start_link(Keyword.t()) :: {:ok, pid()}
  def start_link(opts) do
    connection_args = Keyword.fetch!(opts, :connection)
    conn_str = connection_string(connection_args)

    GenServer.start_link(__MODULE__, [{:conn_str, to_charlist(conn_str)} | opts])
  end

  @doc """
  Sends a query to the ODBC driver.

  `pid` is the `:odbc` process id
  `statement` is the SQL query string
  `opts` are options to be passed on to `:odbc`
  """
  @spec sql_query(pid(), iodata(), Keyword.t()) ::
          {:ok, {:selected, [binary()], [tuple()]}}
          | {:ok, {:selected, [binary()], [tuple()], [{binary()}]}}
          | {:ok, {:updated, non_neg_integer()}}
          | {:error, Error.t()}
  def sql_query(pid, statement, opts \\ []) do
    if Process.alive?(pid) do
      statement = IO.iodata_to_binary(statement)
      timeout = Keyword.get(opts, :timeout, @timeout)

      GenServer.call(pid, {:sql_query, %{statement: statement}}, timeout)
    else
      {:error, %Error{message: :no_connection}}
    end
  end

  @doc """
  Sends a parametrized query to the ODBC driver.

  `pid` is the `:odbc` process id
  `statement` is the SQL query string
  `params` are the parameters to send with the SQL query
  `opts` are options to be passed on to `:odbc`
  """
  @spec param_query(pid(), iodata(), Keyword.t(), Keyword.t()) ::
          {:ok, {:selected, [binary()], [tuple()]}}
          | {:ok, {:selected, [binary()], [tuple()], [{binary()}]}}
          | {:ok, {:updated, non_neg_integer()}}
          | {:error, Error.t()}
  def param_query(pid, statement, params, opts \\ []) do
    if Process.alive?(pid) do
      statement = IO.iodata_to_binary(statement)
      timeout = Keyword.get(opts, :timeout, @timeout)

      GenServer.call(pid, {:param_query, %{statement: statement, params: params}}, timeout)
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
  def handle_call({:sql_query, _query}, _from, %{state: :not_connected} = state) do
    {:reply, {:error, :not_connected}, state}
  end

  def handle_call({:sql_query, %{statement: statement}}, _from, %{pid: pid} = state) do
    result =
      case :odbc.sql_query(pid, to_charlist(statement)) do
        {:error, reason} ->
          error = Error.exception(reason)
          Logger.warn("Unable to execute query: #{error.message}")

          {:reply, {:error, error}, state}

        result ->
          {:reply, {:ok, result}, state}
      end

    result
  end

  def handle_call({:param_query, _query}, _from, %{state: :not_connected} = state) do
    {:reply, {:error, :not_connected}, state}
  end

  def handle_call(
        {:param_query, %{statement: statement, params: params}},
        _from,
        %{pid: pid} = state
      ) do
    case :odbc.param_query(pid, to_charlist(statement), params) do
      {:error, reason} ->
        error = Error.exception(reason)
        Logger.warn("Unable to execute query: #{error.message}")

        {:reply, {:error, error}, state}

      result ->
        {:reply, {:ok, result}, state}
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

  ## Helpers

  defp connection_string(connection_args) do
    driver = Application.get_env(:snowflex, :driver)
    connection_args = [{:driver, driver} | connection_args]

    Enum.reduce(connection_args, "", fn {key, value}, acc ->
      acc <> "#{key}=#{value};"
    end)
  end
end
