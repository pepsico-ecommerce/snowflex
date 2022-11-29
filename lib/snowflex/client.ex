defmodule Snowflex.Client do
  @moduledoc """
  Adapter to Erlang's `:odbc` module.

  A GenServer that handles communication between Elixir and Erlang's `:odbc` module.
  """

  use GenServer

  require Logger

  alias Snowflex.Error

  @timeout :timer.seconds(60)

  ## Public API

  @doc """
  Starts the connection process to the ODBC driver.
  """
  @spec start_link(Keyword.t()) :: {:ok, pid()}
  def start_link(opts) do
    conn_str = connection_string(opts)

    GenServer.start_link(__MODULE__, [{:conn_str, to_charlist(conn_str)} | opts])
  end

  @doc """
  Sends a query to the ODBC driver.

  `pid` is the `:odbc` process id
  `statement` is the SQL query string
  `opts` are options to be passed on to `:odbc`
  """
  @spec sql_query(pid(), iodata(), Keyword.t()) ::
          {:ok, {:selected, [binary()], [tuple()]}}
          | {:ok, {:selected, [binary()], [tuple()], [{binary()}]}}
          | {:ok, {:updated, non_neg_integer()}}
          | {:error, Error.t()}
  def sql_query(pid, statement, opts \\ []) do
    if Process.alive?(pid) do
      statement = IO.iodata_to_binary(statement)
      timeout = Keyword.get(opts, :timeout, @timeout)

      GenServer.call(pid, {:sql_query, %{statement: statement}}, timeout)
    else
      {:error, %Error{message: :no_connection}}
    end
  end

  @doc """
  Sends a commit to the ODBC driver.

  `pid` is the `:odbc` process id
  `mode` is either commit | rollback
  `opts` are options to be passed on to `:odbc`
  """
  @spec commit(pid(), :commit | :rollback, Keyword.t()) :: :ok | {:error, Error.t()}
  def commit(pid, mode, opts \\ []) do
    if Process.alive?(pid) do
      timeout = Keyword.get(opts, :timeout, @timeout)
      GenServer.call(pid, {:commit, mode}, timeout)
    else
      {:error, %Error{message: :no_connection}}
    end
  end

  @doc """
  Sends a select_count to the ODBC driver.

  Executes a SQL SELECT query and associates the result set with the connection.
  A cursor is positioned before the first row in the result set and the tuple {:ok, num_rows} is returned.

  `pid` is the `:odbc` process id
  `statement` is the sql query statement
  """
  @spec select_count(pid(), Query.t(), Keyword.t()) :: {:ok, integer()} | {:error, Error.t()}
  def select_count(pid, %{statement: statement} = _query, opts \\ []) do
    if Process.alive?(pid) do
      statement = IO.iodata_to_binary(statement)
      timeout = Keyword.get(opts, :timeout, @timeout)

      GenServer.call(pid, {:select_count, %{statement: statement}}, timeout)
    else
      {:error, %Error{message: :no_connection}}
    end
  end

  @doc """
  Sends a select to the ODBC driver.

  Selects num_rows consecutive rows of the result set. If Position is next it is semantically equivalent of calling next/[1,2] num_rows times. If Position is {:relative, Pos}, Pos will be used as an offset from the current cursor position to determine the first selected row. If Position is {:absolute, Pos}, Pos will be the number of the first row selected. After this function has returned the cursor is positioned at the last selected row. If there is less then N rows left of the result set the length of Rows will be less than N. If the first row to select happens to be beyond the last row of the result set, the returned value will be {selected, ColNames,[]} e.i. the list of row values is empty indicating that there is no more data to fetch.

  `pid` is the `:odbc` process id
  `position` can be next | {:relative, postion} | {:absolute, position}
  `num_rows` is the number of rows you wish to select
  """
  @spec select(
          pid(),
          :next | {:absolute, integer()} | {:relative, integer()},
          integer(),
          Keyword.t()
        ) ::
          {:ok, {:selected, [binary()], [tuple()]}}
          | {:error, Error.t()}
  def select(pid, position, num_rows, opts \\ []) do
    if Process.alive?(pid) do
      timeout = Keyword.get(opts, :timeout, @timeout)

      GenServer.call(pid, {:select, position, num_rows}, timeout)
    else
      {:error, %Error{message: :no_connection}}
    end
  end

  @doc """
  Sends a next command to the ODBC driver.

  Returns the next row of the result set relative the current cursor position and positions the cursor at this row. If the cursor is positioned at the last row of the result set when this function is called the returned value will be {selected, ColNames,[]} e.i. the list of row values is empty indicating that there is no more data to fetch.

  `pid` is the `:odbc` process id
  """
  @spec next(pid(), Keyword.t()) :: {:ok, {:selected, [binary()], tuple()}} | {:error, Error.t()}
  def next(pid, opts \\ []) do
    if Process.alive?(pid) do
      timeout = Keyword.get(opts, :timeout, @timeout)

      GenServer.call(pid, :next, timeout)
    else
      {:error, %Error{message: :no_connection}}
    end
  end

  @doc """
  Sends a parametrized query to the ODBC driver.

  `pid` is the `:odbc` process id
  `statement` is the SQL query string
  `params` are the parameters to send with the SQL query
  `opts` are options to be passed on to `:odbc`
  """
  @spec param_query(pid(), iodata(), Keyword.t(), Keyword.t()) ::
          {:ok, {:selected, [binary()], [tuple()]}}
          | {:ok, {:selected, [binary()], [tuple()], [{binary()}]}}
          | {:ok, {:updated, non_neg_integer()}}
          | {:error, Error.t()}
  def param_query(pid, statement, params, opts \\ []) do
    if Process.alive?(pid) do
      statement = IO.iodata_to_binary(statement)
      timeout = Keyword.get(opts, :timeout, @timeout)

      GenServer.call(pid, {:param_query, %{statement: statement, params: params}}, timeout)
    else
      {:error, %Error{message: :no_connection}}
    end
  end

  @doc """
  Disconnects from the ODBC driver.
  """
  @spec disconnect(pid()) :: :ok
  def disconnect(pid) do
    GenServer.stop(pid, :normal)
  end

  ## GenServer callbacks

  @impl GenServer
  def init(opts) do
    send(self(), {:start, opts})

    {:ok, %{backoff: :backoff.init(2, 60), state: :not_connected}}
  end

  @impl GenServer
  def handle_call({:sql_query, _query}, _from, %{state: :not_connected} = state) do
    {:reply, {:error, :not_connected}, state}
  end

  def handle_call({:sql_query, %{statement: statement}}, _from, %{pid: pid} = state) do
    result =
      case :odbc.sql_query(pid, to_charlist(statement)) do
        {:error, reason} ->
          error = Error.exception(reason)
          Logger.warn("Unable to execute query: #{error.message}")

          {:reply, {:error, error}, state}

        result ->
          {:reply, {:ok, result}, state}
      end

    result
  end

  def handle_call({:param_query, _query}, _from, %{state: :not_connected} = state) do
    {:reply, {:error, :not_connected}, state}
  end

  def handle_call(
        {:param_query, %{statement: statement, params: params}},
        _from,
        %{pid: pid} = state
      ) do
    case :odbc.param_query(pid, to_charlist(statement), params) do
      {:error, reason} ->
        error = Error.exception(reason)
        Logger.warn("Unable to execute query: #{error.message}")

        {:reply, {:error, error}, state}

      result ->
        {:reply, {:ok, result}, state}
    end
  end

  def handle_call({:commit, _mode}, _from, %{state: :not_connected} = state) do
    {:reply, {:error, :not_connected}, state}
  end

  def handle_call({:commit, commit_mode}, _from, %{pid: pid} = state) do
    case :odbc.commit(pid, commit_mode) do
      {:error, reason} ->
        error = Error.exception(reason)
        Logger.warn("Commit failed: #{error.message}")

        {:reply, {:error, error}, state}

      :ok ->
        {:reply, :ok, state}
    end
  end

  def handle_call({:select_count, _query}, _from, %{state: :not_connected} = state) do
    {:reply, {:error, :not_connected}, state}
  end

  def handle_call({:select_count, %{statement: statement}}, _from, %{pid: pid} = state) do
    case :odbc.select_count(pid, to_charlist(statement)) do
      {:error, reason} ->
        error = Error.exception(reason)
        Logger.warn("Unable to execute select count: #{error.message}")

        {:reply, {:error, error}, state}

      {:ok, rows} ->
        {:reply, {:ok, rows}, state}
    end
  end

  def handle_call({:select, _, _}, _from, %{state: :not_connected} = state) do
    {:reply, {:error, :not_connected}, state}
  end

  def handle_call({:select, position, num_rows}, _from, %{pid: pid} = state) do
    case :odbc.select(pid, position, num_rows) do
      {:error, reason} ->
        error = Error.exception(reason)
        Logger.warn("Unable to execute next: #{error.message}")

        {:reply, {:error, error}, state}

      result ->
        {:reply, {:ok, result}, state}
    end
  end

  def handle_call(:next, _from, %{state: :not_connected} = state) do
    {:reply, {:error, :not_connected}, state}
  end

  def handle_call(:next, _from, %{pid: pid} = state) do
    case :odbc.next(pid) do
      {:error, reason} ->
        error = Error.exception(reason)
        Logger.warn("Unable to execute next: #{error.message}")

        {:reply, {:error, error}, state}

      result ->
        {:reply, {:ok, result}, state}
    end
  end

  @impl GenServer
  def handle_info({:start, opts}, %{backoff: backoff} = _state) do
    connect_opts =
      opts
      |> Keyword.delete_first(:conn_str)

    case :odbc.connect(opts[:conn_str], connect_opts) do
      {:ok, pid} ->
        {:noreply, %{pid: pid, backoff: :backoff.succeed(backoff), state: :connected}}

      {:error, reason} ->
        Logger.warn("Unable to connect to snowflake: #{inspect(reason)}")

        seconds =
          backoff
          |> :backoff.get()
          |> :timer.seconds()

        Process.send_after(self(), {:start, opts}, seconds)

        {_, new_backoff} = :backoff.fail(backoff)

        {:noreply, %{backoff: new_backoff, state: :not_connected}}
    end
  end

  @impl GenServer
  def terminate(_reason, %{state: :not_connected} = _state), do: :ok
  def terminate(_reason, %{pid: pid} = _state), do: :odbc.disconnect(pid)

  ## Helpers

  defp connection_string(connection_args) do
    driver = Application.get_env(:snowflex, :driver)
    connection_args = [{:driver, driver} | connection_args]

    Enum.reduce(connection_args, "", fn {key, value}, acc ->
      acc <> "#{key}=#{value};"
    end)
  end
end
