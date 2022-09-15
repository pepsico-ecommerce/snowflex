defmodule Snowflex.Client.Mock do
  @moduledoc """
  Adapter to Erlang's `:odbc` module.

  A GenServer that handles communication between Elixir and Erlang's `:odbc` module.
  """

  use Snowflex.Client
  use GenServer

  alias Snowflex.Client.Mock.BaseResponser
  alias Snowflex.Error

  require Logger

  @timeout :timer.seconds(60)

  ## Public API

  @doc """
  Starts the connection process
  """
  @impl Snowflex.Client
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @doc """
  Sends a query to the ODBC driver.

  `pid` is the `:odbc` process id
  `statement` is the SQL query string
  `opts` are options to be passed on to `:odbc`
  """
  @impl Snowflex.Client
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
  @impl Snowflex.Client
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
  @impl Snowflex.Client
  def disconnect(pid) do
    GenServer.stop(pid, :normal)
  end

  ## GenServer callbacks

  @impl GenServer
  def init(opts) do
    send(self(), {:start, opts})

    {:ok, %{state: :not_connected}}
  end

  @impl GenServer
  def handle_call({:sql_query, _query}, _from, %{state: :not_connected} = state) do
    {:reply, {:error, :not_connected}, state}
  end

  @impl GenServer
  def handle_call({:sql_query, %{statement: statement}}, _from, state) do
    {:reply, {:ok, handle_mock(statement)}, state}
  end

  @impl GenServer
  def handle_call({:param_query, _query}, _from, %{state: :not_connected} = state) do
    {:reply, {:error, :not_connected}, state}
  end

  @impl GenServer
  def handle_call(
        {:param_query, %{statement: statement, params: params}},
        _from,
        state
      ) do
    {:reply, {:ok, handle_mock(statement, params)}, state}
  end

  def handle_mock(query, params \\ %{}) do
    mock_responser = Application.get_env(:snowflex, :mock_responser, BaseResponser)

    mock_responser.handle_response(query, params)
  end

  @impl GenServer
  def handle_info({:start, _opts}, _state) do
    {:noreply, %{state: :connected}}
  end

  @impl GenServer
  def terminate(_reason, %{state: :not_connected} = _state), do: :ok
  def terminate(_reason, _state), do: :ok
end

