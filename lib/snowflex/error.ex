defmodule Snowflex.Error do
  @moduledoc """
  Error module for Snowflex.
  """

  defexception [:message, :code, :sql_state, :metadata]

  @typedoc """
  The error type for Snowflex.

  ## Fields

  - `message`: The error message.
  - `code`: The error code (from Snowflake).
  - `sql_state`: The SQL state (from Snowflake).
  - `metadata`: The metadata.  Includes the request and response from the Snowflake API, as well as the options passed to the query.
  """
  @type t :: %__MODULE__{
          message: String.t(),
          code: String.t(),
          sql_state: String.t(),
          metadata: any()
        }

  @spec exception(term()) :: Exception.t()
  def exception(message) do
    %__MODULE__{
      message: to_string(message)
    }
  end
end
