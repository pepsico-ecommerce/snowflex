defmodule Snowflex.Transport do
  @moduledoc """
  Behaviour module that defines the interface for Snowflake transport implementations.

  See `Snowflex.Transport.Http` for the default implementation.
  """

  alias Snowflex.Error
  alias Snowflex.Result

  @type query_result :: {:ok, Result.t()} | {:error, Error.t()}
  @type connection_opts :: Keyword.t()
  @type cursor :: term()
  @type fetch_result :: {:ok, Result.t()} | {:halt, Result.t()} | {:error, Error.t()}

  @callback start_link(connection_opts()) :: GenServer.on_start()
  @doc """
  Execute a statement.  See `c:DBConnection.handle_execute/4` for more information.
  """
  @callback execute_statement(pid(), String.t(), any(), Keyword.t()) :: query_result()
  @doc """
  Declare a statement (primarily for streaming) and return a cursor.
  See `c:DBConnection.handle_declare/4` for more information.
  """
  @callback declare(pid(), String.t(), any(), Keyword.t()) ::
              {:ok, cursor()} | {:error, Error.t()}
  @doc """
  Fetch the next result from a cursor. Return `{:ok, result}` while the cursor
  may still hold more results and `{:halt, result}` once it is exhausted.
  See `c:DBConnection.handle_fetch/4` for more information.
  """
  @callback fetch(pid(), cursor(), Keyword.t()) :: fetch_result()
  @doc """
  Disconnect from the database.  See `c:DBConnection.handle_close/3` for more information.
  """
  @callback disconnect(pid()) :: :ok

  @doc """
  Clean up cursor state after streaming is complete. See `c:DBConnection.handle_deallocate/4` for more information.
  """
  @callback deallocate(pid()) :: :ok

  @doc """
  Periodic pings to the server, default is once per second. See `c:DBConnection.ping/1` for more information.
  """
  @callback ping(pid()) :: query_result()

  @doc """
  Submit a statement for execution and return as soon as the server acknowledges
  it, without waiting for the statement to finish.

  The returned `t:Snowflex.Result.t/0` carries the server's statement handle in
  `:query_id`; it has no rows. Use `c:statement_status/3` to check on the
  statement later and `c:cancel_statement/3` to stop it.
  """
  @callback submit_async(pid(), String.t(), any(), Keyword.t()) :: query_result()

  @doc """
  Report the status of a previously submitted statement.

  On success the `t:Snowflex.Result.t/0` `:metadata` holds `%{"status" =>
  "running" | "succeeded"}`. A statement that finished with an error is reported
  as `{:error, t:Snowflex.Error.t/0}`.
  """
  @callback statement_status(pid(), String.t(), Keyword.t()) :: query_result()

  @doc """
  Request cancellation of a previously submitted statement.
  """
  @callback cancel_statement(pid(), String.t(), Keyword.t()) :: query_result()

  # Optional so that transports predating async submission (and any custom
  # transport that cannot support it) still satisfy the behaviour. Callers reach
  # these through Snowflex.Connection, which checks for them and raises a
  # descriptive error rather than UndefinedFunctionError.
  @optional_callbacks submit_async: 4, statement_status: 3, cancel_statement: 3
end
