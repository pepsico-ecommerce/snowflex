defmodule Snowflex.DBConnection.Protocol do
  use DBConnection

  require Logger

  alias Snowflex.DBConnection.{
    Query,
    Result,
    Server
  }

  defstruct pid: nil, status: :idle, conn_opts: []

  @type state :: %__MODULE__{
          pid: pid(),
          status: :idle,
          conn_opts: Keyword.t()
        }

  ## DBConnection Callbacks

  @impl DBConnection
  def connect(opts) do
    connection_args = Keyword.fetch!(opts, :connection)
    conn_str = connection_string(connection_args)

    {:ok, pid} = Server.start_link(conn_str, opts)

    state = %__MODULE__{
      pid: pid,
      status: :idle,
      conn_opts: connection_args
    }

    {:ok, state}
  end

  @impl DBConnection
  def disconnect(_err, %{pid: pid}), do: Server.disconnect(pid)

  @impl DBConnection
  def checkout(state), do: {:ok, state}

  @impl DBConnection
  def handle_begin(_opts, _state) do
    throw("not implemeted")
  end

  @impl DBConnection
  def ping(state) do
    query = %Query{name: "ping", statement: "SELECT /* snowflex:heartbeat */ 1;"}

    case do_query(query, [], [], state) do
      {:ok, _, new_state} -> {:ok, new_state}
      {:error, reason, new_state} -> {:disconnect, reason, new_state}
    end
  end

  ## Helpers

  defp connection_string(connection_args) do
    driver = Application.get_env(:snowflex, :driver)
    connection_args = [{:driver, driver} | connection_args]

    Enum.reduce(connection_args, "", fn {key, value}, acc ->
      acc <> "#{key}=#{value};"
    end)
  end

  # TODO add updated result clause
  defp do_query(%Query{} = query, params, opts, state) do
    case Server.query(state.pid, query.statement, params, opts, true) do
      {:selected, columns, rows, _} ->
        result = %Result{
          columns: Enum.map(columns, &to_string(&1)),
          rows: rows,
          num_rows: Enum.count(rows)
        }

        {:ok, result, state}

      {:error, reason} ->
        {:error, reason, state}
    end
  end
end
