defmodule Snowflex.Connection do
  @moduledoc """
  DBConnection implementation for Snowflex.
  """

  use DBConnection

  alias Snowflex.Error
  alias Snowflex.Result
  alias Snowflex.Transport.Http

  require Logger

  @type t :: %__MODULE__{
          pid: pid(),
          transport: any(),
          state: :not_connected | :connected,
          opts: Keyword.t()
        }
  defstruct [:pid, :transport, :opts, state: :not_connected]

  ## DBConnection Callbacks

  @impl DBConnection
  def connect(opts) do
    Process.flag(:trap_exit, true)
    transport = Keyword.get(opts, :transport, Http)

    case transport.start_link(opts) do
      {:ok, pid} ->
        conn_state = %__MODULE__{
          pid: pid,
          transport: transport,
          state: :connected,
          opts: opts
        }

        # Set base metadata immediately upon connection
        set_base_metadata(conn_state)

        {:ok, conn_state}

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
  def ping(%{transport: transport, pid: pid} = state) do
    set_base_metadata(state)

    case transport.ping(pid) do
      {:ok, _result} ->
        {:ok, state}

      {:error, reason} ->
        enrich_logger_metadata_from_error(reason)
        {:disconnect, reason, state}
    end
  end

  @impl DBConnection
  def handle_prepare(query, _opts, state) do
    {:ok, query, state}
  end

  @impl DBConnection
  def handle_execute(query, params, opts, %{transport: transport} = state) do
    # Set base metadata at the start so it's available even if DBConnection times out
    set_base_metadata(state, query)

    case transport.execute_statement(state.pid, query.statement, params, opts) do
      {:ok, result} ->
        {:ok, query, result, state}

      {:error, reason} ->
        enrich_logger_metadata_from_error(reason)
        {:error, reason, state}
    end
  end

  @impl DBConnection
  def handle_close(_query, _opts, state) do
    {:ok, %Result{}, state}
  end

  @impl DBConnection
  def handle_declare(query, params, opts, %{transport: transport} = state) do
    # Set base metadata at the start so it's available even if DBConnection times out
    set_base_metadata(state, query)

    case transport.declare(state.pid, query.statement, params, opts) do
      {:ok, cursor} ->
        {:ok, query, cursor, state}

      {:error, reason} ->
        enrich_logger_metadata_from_error(reason)
        {:error, reason, state}
    end
  end

  @impl DBConnection
  def handle_deallocate(_query, _cursor, _opts, state) do
    {:ok, state.pid, state}
  end

  @impl DBConnection
  def handle_fetch(query, cursor, opts, %{transport: transport} = state) do
    # Set base metadata at the start so it's available even if DBConnection times out
    set_base_metadata(state, query)

    case transport.fetch(state.pid, cursor, opts) do
      {:cont, result} ->
        {:cont, result, query, state}

      {:halt, result} ->
        {:halt, result, state}

      {:error, reason} ->
        enrich_logger_metadata_from_error(reason)
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

  ## Helpers

  defp set_base_metadata(state) do
    set_base_metadata(state, %{})
  end

  defp set_base_metadata(%__MODULE__{opts: opts}, query) do
    # Set base connection metadata that should be available for all logs
    # This is called at the start of each request so metadata is present
    # even if DBConnection times out before our code completes
    [
      snowflex_account_name: Keyword.get(opts, :account_name),
      snowflex_username: Keyword.get(opts, :username),
      snowflex_warehouse: Keyword.get(opts, :warehouse),
      snowflex_role: Keyword.get(opts, :role),
      snowflex_database: Keyword.get(opts, :database),
      snowflex_schema: Keyword.get(opts, :schema),
      snowflex_statement: Map.get(query, :statement, "")
    ]
    |> Logger.metadata()
  end

  defp enrich_logger_metadata_from_error(%Error{metadata: metadata}) when is_map(metadata) do
    # Extract query_id from error metadata for logging
    case Map.get(metadata, :query_id) do
      nil -> :ok
      query_id -> Logger.metadata(snowflex_query_id: query_id)
    end
  end

  defp enrich_logger_metadata_from_error(_), do: :ok
end
