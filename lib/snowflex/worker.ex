defmodule Snowflex.Worker do
  @moduledoc false

  require Logger
  use GenServer

  @timeout :timer.seconds(60)
  @string_types [:sql_char, :sql_wchar, :sql_varchar, :sql_wvarchar, :sql_wlongvarchar]

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
  def init(connection_args) do
    send(self(), {:start, connection_args})
    {:ok, %{backoff: :backoff.init(2, 60), state: :not_connected}}
  end

  def handle_call({:sql_query, _query}, _from, state = %{state: :not_connected}) do
    {:reply, {:err, :not_connected}, state}
  end

  def handle_call({:sql_query, query}, _from, state = %{pid: pid}) do
    case :odbc.sql_query(pid, to_charlist(query)) do
      {:error, reason} ->
        Logger.warn("Unable to execute query: #{reason}")
        {:reply, {:error, reason}, state}

      result ->
        {:reply, {:ok, result}, state}
    end
  end

  def handle_call({:param_query, _query, _params}, _from, state = %{state: :not_connected}) do
    {:reply, {:err, :not_connected}, state}
  end

  def handle_call({:param_query, query, params}, _from, state = %{pid: pid}) do
    ch_query = to_charlist(query)
    ch_params = prepare_params(params)

    case :odbc.param_query(pid, ch_query, ch_params) do
      {:error, reason} ->
        Logger.warn("Unable to execute query: #{reason}")
        {:reply, {:error, reason}, state}

      result ->
        {:reply, {:ok, result}, state}
    end
  end

  def handle_info({:start, connection_args}, %{backoff: backoff}) do
    conn_str = connection_string(connection_args)

    case :odbc.connect(conn_str, []) do
      {:ok, pid} ->
        {:noreply, %{pid: pid, backoff: :backoff.succeed(backoff), state: :connected}}

      {:error, reason} ->
        Logger.warn("Unable to connect to snowflake: #{reason}")
        Process.send_after(self(), :start, backoff |> :backoff.get() |> :timer.seconds())
        {_, new_backoff} = :backoff.fail(backoff)
        {:noreply, %{backoff: new_backoff, state: :not_connected}}
    end
  end

  # Helpers

  defp connection_string(connection_args) do
    driver = Application.get_env(:snowflex, :driver)
    connection_args = [{:driver, driver} | connection_args]

    connection_args
    |> Enum.reduce("", fn {key, value}, acc -> acc <> "#{key}=#{value};" end)
    |> to_charlist()
  end

  defp prepare_params(params) do
    Enum.map(params, &prepare_param/1)
  end

  defp prepare_param({type, values}) when not is_list(values) do
    prepare_param({type, [values]})
  end

  defp prepare_param({{type_atom, _size} = type, values}) when type_atom in @string_types do
    {type, Enum.map(values, &to_charlist/1)}
  end

  defp prepare_param(param), do: param
end
