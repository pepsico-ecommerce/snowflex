defmodule Snowflex.Error do
  @moduledoc """
  Defines an error returned from the ODBC adapter.
  """

  defexception [:message]

  @type t :: %__MODULE__{
          message: String.t()
        }

  @spec exception(term()) :: t()
  def exception({odbc_code, native_code, reason}) do
    message =
      to_string(reason) <>
        " - ODBC_CODE: " <>
        to_string(odbc_code) <>
        " - SNOWFLAKE_CODE: " <> to_string(native_code)

    %__MODULE__{
      message: message
    }
  end

  def exception(message) do
    %__MODULE__{
      message: to_string(message)
    }
  end
end
