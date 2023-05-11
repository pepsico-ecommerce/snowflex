defmodule Snowflex.Type do
  @moduledoc """
  Type conversions.
  """

  @typedoc "Input param."
  @type param ::
          bitstring()
          | number()
          | datetime()
          | Decimal.t()
          | Date.t()
          | Time.t()
          | DateTime.t()
          | NaiveDateTime.t()

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

  @doc """
  Transforms input params into `:odbc` params.
  """
  def encode(true, _), do: {:sql_integer, [1]}
  def encode(false, _), do: {:sql_integer, [0]}

  def encode(nil, _) do
    {:sql_integer, [:null]}
  end

  def encode(param, _) when is_integer(param), do: {:sql_integer, [param]}

  def encode(param, _) when is_float(param), do: {:sql_double, [param]}

  def encode(param, _) when is_atom(param) do
    encoded = Atom.to_string(param)
    {{:sql_varchar, String.length(encoded)}, [encoded]}
  end

  def encode(%Decimal{} = param, _) do
    encoded = to_string(param)
    {{:sql_varchar, String.length(encoded)}, [encoded]}
  end

  def encode(%Date{} = param, _) do
    encoded = Date.to_iso8601(param)
    {{:sql_varchar, String.length(encoded)}, [encoded]}
  end

  def encode(%Time{} = param, _) do
    encoded = Time.to_iso8601(param)
    {{:sql_varchar, String.length(encoded)}, [encoded]}
  end

  def encode(%DateTime{} = param, _) do
    encoded = DateTime.to_iso8601(param)
    {{:sql_varchar, String.length(encoded)}, [encoded]}
  end

  def encode(%NaiveDateTime{} = param, _) do
    encoded = NaiveDateTime.to_iso8601(param)
    {{:sql_varchar, String.length(encoded)}, [encoded]}
  end

  def encode(param, _opts) when is_binary(param) do
    case :unicode.characters_to_binary(param, :unicode, {:utf16, :little}) do
      utf16 when is_bitstring(utf16) ->
        {{:sql_wvarchar, byte_size(param)}, [utf16]}

      _ ->
        raise "Snowflex failed to convert string to UTF16LE: #{param}"
    end
  end

  def encode(value, _) do
    raise Snowflex.Error.exception(
            message: "could not parse param #{inspect(value)} of unrecognised type."
          )
  end

  @doc """
  Transforms `:odbc` return values to Elixir representations.
  """
  @spec decode(:odbc.value(), opts :: Keyword.t()) :: return_value()

  def decode(:null, _) do
    nil
  end

  def decode(value, _opts) do
    value
  end
end
