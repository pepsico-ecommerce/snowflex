defmodule Snowflex.Worker do
  @moduledoc false

  require Logger
  use GenServer

  @timeout :timer.seconds(60)
  @gc_delay_ms 5
  @string_types ~w(
    sql_char
    sql_wchar
    sql_varchar
    sql_wvarchar
    sql_wlongvarchar
  )a

  @sql_start [:snowflex, :sql_query, :start]
  @sql_stop [:snowflex, :sql_query, :stop]
  @param_start [:snowflex, :param_query, :start]
  @param_stop [:snowflex, :param_query, :stop]

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
    start_time = System.monotonic_time()
    :telemetry.execute(@sql_start, %{system_time: System.system_time()}, %{query: query})

    {result, state} =
      state
      |> do_sql_query(query)
      |> reschedule_heartbeat()

    duration = System.monotonic_time() - start_time
    :telemetry.execute(@sql_stop, %{duration: duration})
    Process.send_after(self(), :gc, @gc_delay_ms)
    {:reply, result, state}
  end

  def handle_call({:param_query, query, params}, _from, state) do
    start_time = System.monotonic_time()

    :telemetry.execute(@param_start, %{system_time: System.system_time()}, %{
      query: query,
      params: params
    })

    {result, state} =
      state
      |> do_param_query(query, params)
      |> reschedule_heartbeat()

    duration = System.monotonic_time() - start_time
    :telemetry.execute(@param_stop, %{duration: duration})
    {:reply, result, state}
  end

  @impl GenServer
  def handle_info({:start, connection_args, keep_alive?, heartbeat_interval}, %{backoff: backoff}) do
    conn_str = connection_string(connection_args)

    case :odbc.connect(conn_str, []) do
      {:ok, pid} ->
        state =
          %{
            pid: pid,
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
    {:no_reply, state}
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
    ch_params = prepare_params(params)

    case :odbc.param_query(pid, ch_query, ch_params) do
      {:error, reason} ->
        Logger.warn("Unable to execute query: #{reason}")
        {{:error, reason}, state}

      result ->
        {{:ok, result}, state}
    end
  end

  defp prepare_params(params) do
    Enum.map(params, &prepare_param/1)
  end

  defp prepare_param({type, values}) when not is_list(values) do
    prepare_param({type, [values]})
  end

  defp prepare_param({{type_atom, _size} = type, values}) when type_atom in @string_types do
    {type, Enum.map(values, &null_or_charlist/1)}
  end

  defp prepare_param({type, values}) do
    {type, Enum.map(values, &null_or_any/1)}
  end

  defp null_or_charlist(nil) do
    :null
  end

  defp null_or_charlist(val) do
    to_charlist(val)
  end

  defp null_or_any(nil) do
    :null
  end

  defp null_or_any(any) do
    any
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
