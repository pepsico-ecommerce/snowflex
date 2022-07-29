defmodule Snowflex.Connection do
  use DBConnection

  require Logger

  alias Snowflex.EctoAdapter.{
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
    connection_args = Keyword.fetch!(opts, :connection)

    {:ok, pid} = Client.start_link(opts)

    state = %__MODULE__{
      pid: pid,
      status: :idle,
      conn_opts: connection_args
    }

    {:ok, state}
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

  ## Not implemented Callbacks

  @impl DBConnection
  def handle_begin(_opts, _state) do
    throw("not implemented")
  end

  @impl DBConnection
  def handle_commit(_opts, _state) do
    throw("not implemented")
  end

  @impl DBConnection
  def handle_rollback(_opts, _state) do
    throw("not implemented")
  end

  @impl DBConnection
  def handle_declare(_query, _params, _opts, _state) do
    throw("not implemeted")
  end

  @impl DBConnection
  def handle_deallocate(_query, _cursor, _opts, _state) do
    throw("not implemeted")
  end

  @impl DBConnection
  def handle_fetch(_query, _cursor, _opts, _state) do
    throw("not implemeted")
  end

  ## Helpers

  defp do_query(%Query{} = query, [], opts, %{worker: worker} = state) do
    case worker.sql_query(state.pid, query.statement, opts) do
      {:ok, result} ->
        result = parse_result(result, query)
        {:ok, query, result, state}

      {:error, reason} ->
        {:error, reason, state}
    end
  end

  defp do_query(%Query{} = query, params, opts, %{worker: worker} = state) do
    case worker.param_query(state.pid, query.statement, params, opts) do
      {:ok, result} ->
        result = parse_result(result, query)
        {:ok, query, result, state}

      {:error, reason} ->
        {:error, reason, state}
    end
  end

  defp parse_result({:selected, columns, rows, _}, query),
    do: parse_result({:selected, columns, rows}, query)

  defp parse_result({:selected, columns, rows}, query) do
    parse_result(columns, rows, query)
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
