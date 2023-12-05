defmodule Snowflex.EctoAdapter do
  use Ecto.Adapters.SQL,
    driver: :snowflex

  @impl true
  def supports_ddl_transaction?, do: false

  @impl true
  def lock_for_migrations(_meta, _opts, _fun) do
    raise "Migrations are not supported"
  end

  def loaders(:integer, type), do: [&int_decode/1, type]
  def loaders(:utc_datetime, type), do: [&datetime_decode/1, type]
  def loaders(:utc_datetime_usec, type), do: [&datetime_decode/1, type]
  def loaders(:naive_datetime, type), do: [&datetime_decode/1, type]
  def loaders(:naive_datetime_usec, type), do: [&datetime_decode/1, type]
  def loaders(:decimal, type), do: [&decimal_decode/1, type]
  def loaders(:float, type), do: [&float_decode/1, type]
  def loaders(:date, type), do: [&date_decode/1, type]
  def loaders(:id, type), do: [&int_decode/1, type]
  def loaders(:time, type), do: [&time_decode/1, type]
  def loaders(:time_usec, type), do: [&time_decode/1, type]

  # NB: Handles instances where date may be wrapped in an additional call, such as `Ecto.Query.API.max/1`
  def loaders({:maybe, :date}, type), do: [&date_decode/1, type]
  def loaders(_, type), do: [type]

  def dumpers(:binary, type), do: [type, &binary_encode/1]
  def dumpers(_, type), do: [type]

  defp binary_encode(raw), do: {:ok, Base.encode16(raw)}

  def decimal_decode(nil), do: {:ok, nil}
  def decimal_decode(dec) when is_binary(dec), do: {:ok, Decimal.new(dec)}
  def decimal_decode(dec) when is_float(dec), do: {:ok, Decimal.from_float(dec)}

  defp int_decode(nil), do: {:ok, nil}
  defp int_decode(int) when is_binary(int), do: {:ok, String.to_integer(int)}
  defp int_decode(int), do: {:ok, int}

  defp time_decode(nil), do: {:ok, nil}
  defp time_decode(time), do: Time.from_iso8601(time)

  defp datetime_decode(nil), do: {:ok, nil}

  defp datetime_decode({date, time}) do
    with {:ok, date} <- Date.from_erl(date),
         {:ok, time} <- Time.from_erl(time) do
      DateTime.new(date, time)
    else
      err -> err
    end
  end

  defp float_decode(nil), do: {:ok, nil}
  defp float_decode(float) when is_float(float), do: float

  defp float_decode(float) do
    {val, _} = Float.parse(float)
    {:ok, val}
  end

  defp date_decode(nil), do: {:ok, nil}
  defp date_decode(date), do: Date.from_iso8601(date)
end
