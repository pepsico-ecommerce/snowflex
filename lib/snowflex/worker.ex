defmodule Snowflex.Worker do
  @moduledoc false

  require Logger

  use GenServer

  alias Snowflex.Params
  alias Snowflex.Telemetry

  @timeout :timer.seconds(60)
  @gc_delay_ms 5

  def start_link(connection_args) do
    GenServer.start_link(__MODULE__, connection_args, [])
  end

  def sql_query(pid, query, timeout \\ @timeout) do
    GenServer.call(pid, {:sql_query, query}, timeout)
  end

  def param_query(pid, query, params, timeout \\ @timeout) do
    GenServer.call(pid, {:param_query, query, params}, timeout)
  end

  ## GENSERVER CALL BACKS

  @impl GenServer
  def init(
        connection_args: connection_args,
        keep_alive?: keep_alive?,
        heartbeat_interval: heartbeat_interval
      ) do
    send(self(), {:start, connection_args, keep_alive?, heartbeat_interval})
    {:ok, %{backoff: :backoff.init(2, 60), state: :not_connected}}
  end

  @impl GenServer
  def handle_call({:sql_query, query}, _from, state) do
    start_time = Telemetry.sql_start(%{query: query})

    {result, state} =
      state
      |> do_sql_query(query)
      |> reschedule_heartbeat()

    Telemetry.sql_stop(start_time)

    Process.send_after(self(), :gc, @gc_delay_ms)
    {:reply, result, state}
  end

  def handle_call({:param_query, query, params}, _from, state) do
    start_time = Telemetry.param_start(%{query: query, params: params})

    {result, state} =
      state
      |> do_param_query(query, params)
      |> reschedule_heartbeat()

    Telemetry.param_stop(start_time)

    {:reply, result, state}
  end

  @impl GenServer
  def handle_info({:start, connection_args, keep_alive?, heartbeat_interval}, %{backoff: backoff}) do
    case Snowflex.transport().connect(connection: connection_args) do
      {:ok, conn} ->
        state =
          %{
            conn: conn,
            backoff: :backoff.succeed(backoff),
            state: :connected,
            keep_alive?: keep_alive?,
            heartbeat_interval: heartbeat_interval
          }
          |> schedule_heartbeat()

        {:noreply, state}

      {:error, reason} ->
        Logger.warn("Unable to connect to snowflake: #{reason}")

        Process.send_after(
          self(),
          {:start, connection_args, keep_alive?, heartbeat_interval},
          backoff |> :backoff.get() |> :timer.seconds()
        )

        {_, new_backoff} = :backoff.fail(backoff)
        {:noreply, %{backoff: new_backoff, state: :not_connected}}
    end
  end

  def handle_info(:send_heartbeat, state) do
    {:noreply, state |> send_heartbeat() |> schedule_heartbeat()}
  end

  def handle_info(:gc, state) do
    :erlang.garbage_collect(self())
    {:noreply, state}
  end

  # Helpers

  defp do_sql_query(%{state: :not_connected} = state, _query) do
    {{:error, :not_connected}, state}
  end

  defp do_sql_query(%{conn: conn} = state, query) do
    case Snowflex.transport().sql_query(conn, query) do
      {:ok, result} ->
        {{:ok, result}, state}

      {:error, reason} ->
        Logger.warn("Unable to execute query: #{reason}")
        {{:error, reason}, state}
    end
  end

  defp do_param_query(%{state: :not_connected} = state, _query, _params) do
    {{:error, :not_connected}, state}
  end

  defp do_param_query(%{conn: conn} = state, query, params) do
    params = Params.prepare(params)

    case Snowflex.transport().param_query(conn, query, params) do
      {:ok, result} ->
        {{:ok, result}, state}

      {:error, reason} ->
        Logger.warn("Unable to execute query: #{reason}")
        {{:error, reason}, state}
    end
  end

  defp send_heartbeat(state) do
    Logger.info("sending heartbeat")

    state
    |> do_sql_query("SELECT 1")
    |> log_heartbeat_result()
  end

  defp log_heartbeat_result({{:ok, _result}, state}) do
    Logger.info("heartbeat sent successfully")
    state
  end

  defp log_heartbeat_result({{:error, _reason}, state}) do
    Logger.warn("heartbeat failed to send")
    state
  end

  defp schedule_heartbeat(%{keep_alive?: true, heartbeat_interval: interval} = state) do
    Logger.info("scheduling next heartbeat in #{interval}ms")
    ref = Process.send_after(self(), :send_heartbeat, interval)
    Map.put(state, :heartbeat_ref, ref)
  end

  defp schedule_heartbeat(state), do: state

  # only reschedule if there were no errors
  defp reschedule_heartbeat({
         {:ok, _} = result,
         %{keep_alive?: true, heartbeat_ref: old_ref} = state
       }) do
    Process.cancel_timer(old_ref)
    {result, schedule_heartbeat(state)}
  end

  defp reschedule_heartbeat({result, state}), do: {result, state}
end
