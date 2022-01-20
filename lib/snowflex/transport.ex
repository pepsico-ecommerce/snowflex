defmodule Snowflex.Transport do
  @moduledoc false

  alias Snowflex.Result

  @typedoc "An opaque type representing a transport's active connection (if any)."
  @type conn :: any()

  @typedoc """
  An opaque type representing a typed query parameter.

  Each transport provides helper functions for constructing values of this type,
  and your connection module (wherever you `use Snowflex.Connection`) will also
  automatically import the helpers corresponding to the active transport.
  """
  @type param :: any()

  @doc "Establish a connection to the transport."
  @callback connect(connection_args :: Keyword.t()) :: {:ok, conn()} | {:error, term()}

  @doc """
  Close an active connection to the transport.

  This callback will receive the `conn` returned on success from `c:connect/1`.
  """
  @callback disconnect(conn()) :: :ok | {:error, term()}

  @typedoc "Query result row (non-null row values are always returned as strings)."
  @type row :: %{String.t() => String.t() | :null}

  @typedoc "The result of performing a query."
  @type query_result :: {:ok, Result.t()} | {:ok, [Result.t()]} | {:error, term()}

  @doc "Perform a query against a given connection."
  @callback sql_query(conn(), query :: String.t()) :: query_result()

  @doc "Perorm a parametrized query against a given connection."
  @callback param_query(conn(), query :: String.t(), params :: [param()]) :: query_result()
end
