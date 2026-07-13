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
end
