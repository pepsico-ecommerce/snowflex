defmodule Snowflex.Connection do
  use DBConnection

  require Logger

  alias Snowflex.{
    Query,
    Result,
    Client
  }

  defstruct pid: nil, status: :idle, conn_opts: [], worker: Client

  @type state :: %__MODULE__{
          pid: pid(),
          status: :idle,
          conn_opts: Keyword.t(),
          worker: Client | any()
        }

  ## DBConnection Callbacks

  @impl DBConnection
  def connect(opts) do
    connection_args =
      opts
      |> Keyword.fetch!(:connection)
      |> set_defaults()

    {:ok, pid} = Client.start_link(opts |> set_defaults())

    state = %__MODULE__{
      pid: pid,
      status: :idle,
      conn_opts: connection_args
    }

    {:ok, state}
  end

  defp set_defaults(opts) do
    [auto_commit: :on, binary_strings: :on, tuple_row: :off, extended_errors: :on]
    |> Keyword.merge(opts)
  end

  @impl DBConnection
  def disconnect(_err, %{pid: pid}), do: Client.disconnect(pid)

  @impl DBConnection
  def checkout(state), do: {:ok, state}

  @impl DBConnection
  def ping(state) do
    query = %Query{name: "ping", statement: "SELECT /* snowflex:heartbeat */ 1;"}

    case do_query(query, [], [], state) do
      {:ok, _, _, new_state} -> {:ok, new_state}
      {:error, reason, new_state} -> {:disconnect, reason, new_state}
    end
  end

  @impl DBConnection
  def handle_prepare(query, _opts, state) do
    {:ok, query, state}
  end

  @impl DBConnection
  def handle_execute(query, params, opts, state) do
    do_query(query, params, opts, state)
  end

  @impl DBConnection
  def handle_status(_, %{status: {status, _}} = state), do: {status, state}
  def handle_status(_, %{status: status} = state), do: {status, state}

  @impl DBConnection
  def handle_close(_query, _opts, state) do
    {:ok, %Result{}, state}
  end

  @impl DBConnection
  def handle_begin(
        _opts,
        %Snowflex.Connection{
          conn_opts: opts
        } = state
      ) do
    case Keyword.get(opts, :auto_commit) do
      :off ->
        {:ok, %Result{}, state}

      _ ->
        {:error, "auto_commit must be off for the connection to use transactions"}
    end
  end

  @impl DBConnection
  def handle_commit(opts, %{worker: worker} = state) do
    case worker.commit(state.pid, :commit, opts) do
      :ok -> {:ok, nil, state}
      {:error, reason} -> {:error, reason, state}
    end
  end

  @impl DBConnection
  def handle_rollback(opts, %{worker: worker} = state) do
    case worker.commit(state.pid, :rollback, opts) do
      :ok -> {:ok, nil, state}
      {:error, reason} -> {:error, reason, state}
    end
  end

  @impl DBConnection
  def handle_declare(query, _params, _opts, %{worker: worker} = state) do
    case worker.select_count(state.pid, query) do
      {:ok, _result} ->
        {:ok, query, state.pid, state}

      {:error, reason} ->
        {:error, reason, state}
    end
  end

  @impl DBConnection
  def handle_deallocate(_query, _cursor, _opts, state) do
    {:ok, state.pid, state}
  end

  @impl DBConnection
  def handle_fetch(query, cursor, opts, %{worker: worker} = state) do
    max_rows = Keyword.get(opts, :max_rows, 500)

    case worker.select(cursor, :next, max_rows) do
      {:ok, result} ->
        result = parse_result(result, query)

        if result.rows == [] do
          {:halt, result, state}
        else
          {:cont, result, state}
        end

      {:error, reason} ->
        {:error, reason, state}
    end
  end

  ## Helpers

  defp do_query(%Query{} = query, params, opts, state) do
    if Keyword.get(state.conn_opts, :auto_commit, :on) do
      do_query_without_commit(query, params, opts, state)
    else
      do_query_with_commit(query, params, opts, state)
    end
  end

  defp do_query_without_commit(%Query{} = query, [], opts, %{worker: worker} = state) do
    with {:ok, result} <- worker.sql_query(state.pid, query.statement, opts) do
      result = parse_result(result, query)
      {:ok, query, result, state}
    else
      {:error, reason} ->
        {:error, reason, state}
    end
  end

  defp do_query_without_commit(%Query{} = query, params, opts, %{worker: worker} = state) do
    with {:ok, result} <- worker.param_query(state.pid, query.statement, params, opts) do
      result = parse_result(result, query)
      {:ok, query, result, state}
    else
      {:error, reason} ->
        {:error, reason, state}
    end
  end

  defp do_query_with_commit(%Query{} = query, [], opts, %{worker: worker} = state) do
    with {:ok, result} <- worker.sql_query(state.pid, query.statement, opts),
         :ok <- worker.commit(state.pid, :commit, opts) do
      result = parse_result(result, query)
      {:ok, query, result, state}
    else
      {:error, reason} ->
        {:error, reason, state}
    end
  end

  defp do_query_with_commit(%Query{} = query, params, opts, %{worker: worker} = state) do
    with {:ok, result} <- worker.param_query(state.pid, query.statement, params, opts),
         :ok <- worker.commit(state.pid, :commit, opts) do
      result = parse_result(result, query)
      {:ok, query, result, state}
    else
      {:error, reason} ->
        {:error, reason, state}
    end
  end

  defp parse_result({:selected, columns, rows, _}, query),
    do: parse_result({:selected, columns, rows}, query)

  defp parse_result({:selected, columns, rows}, query) do
    parse_result(columns, rows, query)
  end

  defp parse_result({:updated, count}, _query) do
    %Result{num_rows: count}
  end

  defp parse_result(result, _query), do: result

  defp parse_result(columns, rows, query) do
    %Result{
      columns: Enum.map(columns, &to_string(&1)),
      rows: rows,
      num_rows: Enum.count(rows),
      success: true,
      statement: query.statement
    }
  end
end
