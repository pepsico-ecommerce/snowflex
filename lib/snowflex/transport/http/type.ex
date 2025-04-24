defmodule Snowflex.Transport.Http.Type do
  @moduledoc """
  Type conversions for REST transport.

  See: https://docs.snowflake.com/en/developer-guide/sql-api/submitting-requests#using-bind-variables-in-a-statement
  """

  alias Snowflex.Error

  @typedoc "Types that can be encoded for Snowflake"
  @type encodeable ::
          boolean()
          | nil
          | integer()
          | float()
          | atom()
          | Decimal.t()
          | Date.t()
          | Time.t()
          | DateTime.t()
          | NaiveDateTime.t()
          | binary()

  @typedoc "Output value."
  @type return_value ::
          bitstring()
          | integer()
          | date()
          | datetime()
          | Decimal.t()

  @typedoc "Date as `{year, month, day}`"
  @type date :: {1..9_999, 1..12, 1..31}

  @typedoc "Time as `{hour, minute, sec, usec}`"
  @type time :: {0..24, 0..60, 0..60, 0..999_999}

  @typedoc "Datetime"
  @type datetime :: {date(), time()}

  @type encoded_value :: %{type: String.t(), value: String.t() | nil}

  @typedoc "Type information for encoding/decoding"
  @type type_info :: map()

  @doc """
  Transforms input params for the REST API.
  The REST API expects values in JSON format with type specified.

  See: https://docs.snowflake.com/en/developer-guide/sql-api/submitting-requests#using-bind-variables-in-a-statement
  """
  @spec encode(encodeable(), term()) :: encoded_value()
  def encode(true, _), do: %{type: "BOOLEAN", value: "true"}
  def encode(false, _), do: %{type: "BOOLEAN", value: "false"}
  def encode(nil, _), do: %{type: "TEXT", value: nil}

  def encode(param, _) when is_integer(param),
    do: %{type: "FIXED", value: to_string(param)}

  def encode(param, _) when is_float(param),
    do: %{type: "REAL", value: to_string(param)}

  def encode(param, _) when is_atom(param) do
    %{type: "TEXT", value: Atom.to_string(param)}
  end

  def encode(%Decimal{} = param, _) do
    %{type: "FIXED", value: to_string(param)}
  end

  def encode(%Date{} = param, _) do
    %{type: "TEXT", value: Date.to_iso8601(param)}
  end

  def encode(%Time{} = param, _) do
    %{type: "TEXT", value: Time.to_iso8601(param)}
  end

  def encode(%DateTime{} = param, _) do
    %{type: "TEXT", value: DateTime.to_iso8601(param)}
  end

  def encode(%NaiveDateTime{} = param, _) do
    %{type: "TEXT", value: NaiveDateTime.to_iso8601(param)}
  end

  def encode(param, _) when is_binary(param) do
    %{type: "TEXT", value: param}
  end

  def encode(value, _) do
    raise Error.exception(
            message: "could not parse param #{inspect(value)} of unrecognised type."
          )
  end

  @doc """
  Transforms REST API return values to Elixir representations.
  """
  @spec decode(nil, term()) :: nil
  def decode(nil, _), do: nil

  @spec decode(binary(), %{type: type_info()} | %{column: binary()}) :: return_value()
  def decode(value, %{type: type_info}) when is_binary(value) do
    column_type = String.downcase(Map.get(type_info, "type", ""))
    decode_by_type(value, column_type, type_info)
  end

  # Fallback for all other cases
  def decode(value, _), do: value

  # Decode based on column type
  defp decode_by_type(value, "fixed", type_info) do
    # Convert fixed type (numeric) based on scale
    scale = Map.get(type_info, "scale", 0)

    if scale > 0 do
      # Decimal number
      Decimal.new(value)
    else
      # Integer
      String.to_integer(value)
    end
  end

  defp decode_by_type(value, "real", _) do
    case Float.parse(value) do
      {float, _} -> float
      _ -> value
    end
  end

  defp decode_by_type(value, "timestamp_ntz", _) do
    case NaiveDateTime.from_iso8601(value) do
      {:ok, datetime} -> datetime
      _ -> value
    end
  end

  defp decode_by_type(value, type, _) when type in ["timestamp_ltz", "timestamp_tz"] do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _} -> datetime
      _ -> value
    end
  end

  defp decode_by_type(value, "date", _) do
    case Date.from_iso8601(value) do
      {:ok, date} -> date
      _ -> value
    end
  end

  defp decode_by_type(value, "time", _) do
    # Add 'Z' to make it fully ISO8601 compliant
    case Time.from_iso8601("#{value}Z") do
      {:ok, time} -> time
      _ -> value
    end
  end

  defp decode_by_type(value, "boolean", _) do
    value == "true"
  end

  # Default for text and other types - return as is
  defp decode_by_type(value, _, _), do: value
end
