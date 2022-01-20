defmodule Snowflex.Connection do
  @moduledoc """
  When used, the connection expects the `:otp_app` option. You may also define
  a standard timeout. This will default to 60 seconds.

  ```elixir
  defmodule SnowflakeConnection do
    use Snowflex.Connection,
      otp_app: :my_app,
      timeout: :timer.seconds(60),
      keep_alive?: true
  end
  ```

  Configuration should be extended in your config files.

  ```
  # config/prod.exs
  config :my_app, SnowflakeConnection,
    size: [
      max: 10,
      min: 5
    ],
    connection: [
      server: "snowflex.us-east-8.snowflakecomputing.com",
      role: "DEV",
      warehouse: "CUSTOMER_DEV_WH"
    ]
  ```

  The connection will default to using the `Snowflex.Worker` module. You are able to
  define a diferent one for testing/development purposes in your configurations as well.

  ```
  # config/dev.exs
  config :my_app, SnowflakeConnection,
    size: [
      max: 1,
      min: 1
    ],
    worker: MyApp.MockWorker
  ```

  ## Usage

  Ensure the connection is started as part of your application.

  ```elixir
  defmodule MyApp.Application do
    def start(_, _) do
      ...

      children = [
        ...,
        SnowflakeConnection
      ]
    end
  end

  `execute/1`
  ```
  query = "SELECT * FROM foo"

  SnowflakeConnection.execute(query)
  ```

  `execute/2`
  ```
  query = \"""
    SELECT * FROM foo
    WHERE bar = ?
  \"""

  SnowflakeConnection.execute(query, [Snowflex.string_param("baz")])
  ```
  """

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

  @doc false
  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      @behaviour Snowflex.Connection

      # setup compile time config
      otp_app = Keyword.fetch!(opts, :otp_app)
      timeout = Keyword.get(opts, :timeout, :timer.seconds(60))
      map_nulls_to_nil? = Keyword.get(opts, :map_nulls_to_nil?, false)
      keep_alive? = Keyword.get(opts, :keep_alive?, false)

      @otp_app otp_app
      @name __MODULE__
      @default_size [
        max: 10,
        min: 5
      ]
      @keep_alive? keep_alive?
      @heartbeat_interval :timer.hours(3)
      @query_opts [
        timeout: timeout,
        map_nulls_to_nil?: map_nulls_to_nil?
      ]

      # import param helpers for active transport
      @transport Snowflex.transport()
      import @transport, only: [int_param: 1, string_param: 1, string_param: 2]

      def child_spec(_) do
        config = Application.get_env(@otp_app, __MODULE__, [])
        connection = Keyword.get(config, :connection, [])
        worker_module = Keyword.get(config, :worker, Snowflex.Worker)

        user_size_config = Keyword.get(config, :size, [])
        final_size_config = Keyword.merge(@default_size, user_size_config)

        min_pool_size = Keyword.get(final_size_config, :min)
        max_pool_size = Keyword.get(final_size_config, :max)

        opts = [
          {:name, {:local, @name}},
          {:worker_module, worker_module},
          {:size, max_pool_size},
          {:max_overflow, min_pool_size}
        ]

        :poolboy.child_spec(@name, opts,
          connection_args: connection,
          keep_alive?: @keep_alive?,
          heartbeat_interval: @heartbeat_interval
        )
      end
    end
  end

  ## DBConnection Callbacks

  @impl DBConnection
  def connect(opts) do
    connection_args =
      opts
      |> Keyword.fetch!(:connection)
      |> set_defaults()

    {:ok, pid} = Client.start_link(connection_args)

    state = %__MODULE__{
      pid: pid,
      conn_opts:
        Keyword.take(connection_args, [:auto_commit, :binary_string, :tuple_row, :extended_errors]),
      status: :idle
    }

    {:ok, state}
  end

  defp set_defaults(opts) do
    [auto_commit: :on, binary_strings: :on, tuple_row: :off, extended_errors: :on]
    |> Keyword.merge(opts)
  end

  @impl DBConnection
  def disconnect(_err, %{pid: pid, conn_opts: conn_opts}) do
    auto_commit =
      case Keyword.fetch(conn_opts, :auto_commit) do
        {:ok, mode} -> mode
        :error -> :on
      end

    if auto_commit == :off, do: Client.commit(pid, :rollback)

    Client.disconnect(pid)
  end

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
  def handle_declare(query, [], _opts, %{worker: worker} = state) do
    case worker.select_count(state.pid, query) do
      {:ok, _result} ->
        {:ok, query, state.pid, state}

      {:error, reason} ->
        {:error, reason, state}
    end
  end

  def handle_declare(_query, _params, _opts, state) do
    {:error,
     "Snowflex does not support streaming with queries using parameters. Please use Ecto.Query.API.fragment to pass your arguments directly as literal string values.",
     state}
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
    case Keyword.get(state.conn_opts, :auto_commit) do
      :off -> do_query_with_commit(query, params, opts, state)
      _ -> do_query_without_commit(query, params, opts, state)
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

  defp parse_result({:updated, count}, query) do
    %Result{num_rows: count, success: true, statement: query.statement}
  end

  defp parse_result(results, query) when is_list(results),
    do: Enum.map(results, fn result -> parse_result(result, query) end)

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
