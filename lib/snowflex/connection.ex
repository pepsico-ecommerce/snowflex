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

    {:ok, pid} = Client.start_link(opts)

    state = %__MODULE__{
      pid: pid,
      status: :idle,
      conn_opts: connection_args
    }

    {:ok, state}
  end

  defp set_defaults(opts) do
    # may need to set this some other way
    [auto_commit: :on, binary_strings: :on, tuple_row: :on, extended_errors: :on]
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

  ## Not implemented Callbacks

  @impl DBConnection
  def handle_begin(
        _opts,
        %Snowflex.Connection{
          conn_opts: opts
        } = state
      ) do
    IO.inspect(state, label: "state")

    case Keyword.get(opts, :auto_commit) do
      :on ->
        # do we need to temporarily disable this? If so when, and how do we know to re-enable it
        {:ok, %Result{}, state}

      :off ->
        {:ok, %Result{}, state}

      opt ->
        # maybe standardize this somewhere
        {:error,
         "bad auto_commit config: #{inspect(opt)} is not a valid config. Set to :on or :off"}
    end
  end

  @impl DBConnection
  def handle_commit(opts, %{worker: worker} = state) do
    case worker.commit(state.pid, :commit, opts) do
      {:ok, result} -> {:ok, result, state}
      {:error, reason} -> {:error, reason, state}
    end
  end

  @impl DBConnection
  def handle_rollback(opts, %{worker: worker} = state) do
    case worker.commit(state.pid, :rollback, opts) do
      {:ok, result} -> {:ok, result, state}
      {:error, reason} -> {:error, reason, state}
    end
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
      # FIXME: I think we can change this in the connection params
      # https://github.com/pepsico-ecommerce/snowflex/blob/22cf4a3d8161a602fcd0d0acba10d6993c5e3036/lib/snowflex/client.ex#L142
      rows:
        Enum.map(rows, fn row ->
          Tuple.to_list(row)
          # TODO: evaluate whether or not this should be done in the loaders
          # https://github.com/pepsico-ecommerce/snowflex/blob/69991f43a918bb996f2181c1d17d1b42119a4c58/lib/snowflex/ecto/ecto_adapter.ex#L13-L15
          |> Enum.map(fn
            :null -> nil
            x -> x
          end)
        end),
      num_rows: Enum.count(rows),
      success: true,
      statement: query.statement
    }
  end
end
