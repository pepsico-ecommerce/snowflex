defmodule Snowflex.Connection do
  @moduledoc """
  DBConnection implementation for Snowflex.
  """

  use DBConnection

  alias Snowflex.Error
  alias Snowflex.Query
  alias Snowflex.Result
  alias Snowflex.Transport.Http

  require Logger

  @type t :: %__MODULE__{
          pid: pid(),
          transport: any(),
          state: :not_connected | :connected
        }
  defstruct [:pid, :transport, state: :not_connected]

  ## DBConnection Callbacks

  @impl DBConnection
  def connect(opts) do
    Process.flag(:trap_exit, true)
    transport = Keyword.get(opts, :transport, Http)

    case transport.start_link(opts) do
      {:ok, pid} ->
        {:ok, %__MODULE__{pid: pid, transport: transport, state: :connected}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl DBConnection
  def disconnect(_err, %{pid: pid, transport: transport}) do
    transport.disconnect(pid)
  end

  @impl DBConnection
  def checkout(state) do
    {:ok, state}
  end

  @impl DBConnection
  def ping(state) do
    query = %Query{name: "ping", statement: "SELECT /* snowflex:heartbeat */ 1;"}

    case handle_execute(query, [], [], state) do
      {:ok, _query, _result, _state} ->
        {:ok, state}

      {:error, reason, _} ->
        {:disconnect, reason, state}
    end
  end

  @impl DBConnection
  def handle_prepare(query, _opts, state) do
    {:ok, query, state}
  end

  @impl DBConnection
  def handle_execute(query, params, opts, %{transport: transport} = state) do
    case transport.execute_statement(state.pid, query.statement, params, opts) do
      {:ok, result} ->
        {:ok, query, result, state}

      {:error, reason} ->
        {:error, reason, state}
    end
  end

  @impl DBConnection
  def handle_close(_query, _opts, state) do
    {:ok, %Result{}, state}
  end

  @impl DBConnection
  def handle_declare(query, params, opts, %{transport: transport} = state) do
    case transport.declare(state.pid, query.statement, params, opts) do
      {:ok, cursor} ->
        {:ok, query, cursor, state}

      {:error, reason} ->
        {:error, reason, state}
    end
  end

  @impl DBConnection
  def handle_deallocate(_query, _cursor, _opts, state) do
    {:ok, state.pid, state}
  end

  @impl DBConnection
  def handle_fetch(query, cursor, opts, %{transport: transport} = state) do
    case transport.fetch(state.pid, cursor, opts) do
      {:cont, result} ->
        {:cont, result, query, state}

      {:halt, result} ->
        {:halt, result, state}

      {:error, reason} ->
        {:error, reason, state}
    end
  end

  # Transaction Callbacks
  @impl DBConnection
  def handle_begin(_opts, state) do
    {:disconnect, Error.exception("Snowflex does not support transactions"), state}
  end

  @impl DBConnection
  def handle_commit(_opts, state) do
    {:disconnect, Error.exception("Snowflex does not support transactions"), state}
  end

  @impl DBConnection
  def handle_rollback(_opts, state) do
    {:disconnect, Error.exception("Snowflex does not support transactions"), state}
  end

  @impl DBConnection
  def handle_status(_opts, state) do
    {:disconnect, Error.exception("Snowflex does not support transactions"), state}
  end
end
