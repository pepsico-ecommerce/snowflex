defmodule Snowflex.DBConnection.Server do
  @moduledoc "Adapter for communicating with the active transport."

  use GenServer

  require Logger

  alias Snowflex.DBConnection.Error
  alias Snowflex.{Params, Telemetry, Transport}

  @timeout :timer.seconds(60)

  ## Public API

  @doc "Starts the connection process to the transport."
  @spec start_link(Keyword.t()) :: {:ok, pid()}
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @doc """
  Sends a query to the transport.

  ## Options

  - `pid` is the server pid
  - `statement` is the SQL query string
  - `opts` are options to be passed on to the transport
  """
  @spec sql_query(pid(), iodata(), Keyword.t()) :: Transport.query_result()
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
  Sends a parametrized query to the transport.

  ## Options

  - `pid` is the server pid
  - `statement` is the SQL query string
  - `params` are the parameters to send with the SQL query
  - `opts` are options to be passed on to the transport
  """
  @spec param_query(pid(), iodata(), [any()], Keyword.t()) :: Transport.query_result()
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

  def handle_call({:sql_query, %{statement: statement}}, _from, %{conn: conn} = state) do
    start_time = Telemetry.sql_start(%{query: statement})

    result =
      case Snowflex.transport().sql_query(conn, statement) do
        {:ok, result} ->
          {:reply, {:ok, result}, state}

        {:error, reason} ->
          error = Error.exception(reason)
          Logger.warn("Unable to execute query: #{error.message}")

          {:reply, {:error, error}, state}
      end

    Telemetry.sql_stop(start_time)

    result
  end

  def handle_call({:param_query, _query}, _from, %{state: :not_connected} = state) do
    {:reply, {:error, :not_connected}, state}
  end

  def handle_call(
        {:param_query, %{statement: statement, params: params}},
        _from,
        %{conn: conn} = state
      ) do
    start_time = Telemetry.param_start(%{query: statement, params: params})
    params = Params.prepare(params)

    result =
      case Snowflex.transport().param_query(conn, statement, params) do
        {:ok, result} ->
          {:reply, {:ok, result}, state}

        {:error, reason} ->
          error = Error.exception(reason)
          Logger.warn("Unable to execute query: #{error.message}")

          {:reply, {:error, error}, state}
      end

    Telemetry.param_stop(start_time)

    result
  end

  @impl GenServer
  def handle_info({:start, opts}, %{backoff: backoff} = _state) do
    opts =
      opts
      |> Keyword.put_new(:auto_commit, :on)
      |> Keyword.put_new(:binary_strings, :on)
      |> Keyword.put_new(:tuple_row, :on)
      |> Keyword.put_new(:extended_errors, :on)

    case Snowflex.transport().connect(opts) do
      {:ok, conn} ->
        {:noreply, %{conn: conn, backoff: :backoff.succeed(backoff), state: :connected}}

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
  def terminate(_reason, %{conn: conn} = _state), do: Snowflex.transport().disconnect(conn)
end
