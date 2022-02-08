defmodule Snowflex.Worker do
  @moduledoc false

  require Logger

  use GenServer

  alias Snowflex.Params
  alias Snowflex.Results
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

  @spec stream(atom | pid | {atom, any} | {:via, atom, any}, any, :infinity | non_neg_integer) ::
          any
  def stream(pid, query, timeout \\ @timeout)

  def stream(pid, query, timeout) do
    stream(pid, query, &default_fn/1, timeout)
  end

  def stream(pid, query, fun, timeout) do
    GenServer.call(pid, {:stream, query, fun}, timeout)
  end

  ## GENSERVER CALL BACKS

  @impl GenServer
  def init(
        connection_args: connection_args,
        keep_alive?: keep_alive?,
        heartbeat_interval: heartbeat_interval,
        pool_name: pool_name
      ) do
    send(self(), {:start, connection_args, keep_alive?, heartbeat_interval, pool_name})
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
  def handle_call({:stream, query, fun}, _from, state) do
    {result, state} =
      state
      |> do_stream(query, fun)
      |> reschedule_heartbeat()

    Process.send_after(self(), :gc, @gc_delay_ms)
    {:reply, result, state}
  end

  @impl GenServer
  def handle_info({:start, connection_args, keep_alive?, heartbeat_interval, pool_name}, %{
        backoff: backoff
      }) do
    conn_str = connection_string(connection_args)

    case :odbc.connect(conn_str, []) do
      {:ok, pid} ->
        state =
          %{
            pid: pid,
            backoff: :backoff.succeed(backoff),
            state: :connected,
            keep_alive?: keep_alive?,
            heartbeat_interval: heartbeat_interval,
            pool_name: pool_name
          }
          |> schedule_heartbeat()

        {:noreply, state}

      {:error, reason} ->
        Logger.warn("Unable to connect to snowflake: #{reason}")

        Process.send_after(
          self(),
          {:start, connection_args, keep_alive?, heartbeat_interval, pool_name},
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

  defp connection_string(connection_args) do
    driver = Application.get_env(:snowflex, :driver)
    connection_args = [{:driver, driver} | connection_args]

    connection_args
    |> Enum.reduce("", fn {key, value}, acc -> acc <> "#{key}=#{value};" end)
    |> to_charlist()
  end

  defp do_sql_query(%{state: :not_connected} = state, _query) do
    {{:error, :not_connected}, state}
  end

  defp do_sql_query(%{pid: pid} = state, query) do
    case :odbc.sql_query(pid, to_charlist(query)) do
      {:error, reason} ->
        Logger.warn("Unable to execute query: #{reason}")
        {{:error, reason}, state}

      result ->
        {{:ok, result}, state}
    end
  end

  defp do_param_query(%{state: :not_connected} = state, _query, _params) do
    {{:error, :not_connected}, state}
  end

  defp do_param_query(%{pid: pid} = state, query, params) do
    ch_query = to_charlist(query)
    ch_params = Params.prepare(params)

    case :odbc.param_query(pid, ch_query, ch_params) do
      {:error, reason} ->
        Logger.warn("Unable to execute query: #{reason}")
        {{:error, reason}, state}

      result ->
        {{:ok, result}, state}
    end
  end

  defp do_stream(%{pid: pid} = state, query, fun) do
    ch_query = to_charlist(query)

    Stream.resource(
      fn ->
        {:ok, count} = :odbc.select_count(pid, ch_query)
        count
      end,
      fn count ->
        if count == 0 do
          {:halt, count}
        else
          res =
            :odbc.next(pid)
            |> Results.process([])
            |> fun.()

          {[res], count - 1}
        end
      end,
      fn values ->
        :poolboy.checkin(state.pool_name, pid)
        values
      end
    )
  end

  defp do_stream(%{state: :not_connected} = state, _query, _fun) do
    {{:error, :not_connected}, state}
  end

  defp default_fn(n), do: n

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
