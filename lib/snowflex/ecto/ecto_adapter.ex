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
  def loaders(:float, type), do: [&float_decode/1, type]
  def loaders(:date, type), do: [&date_decode/1, type]
  def loaders(:id, type), do: [&int_decode/1, type]
  def loaders(:binary_id, type), do: [&uuid_decode/1, type]
  def loaders(:decimal, type), do: [&decimal_decode/1, type]
  def loaders(:time, type), do: [&time_decode/1, type]
  def loaders(:time_usec, type), do: [&time_decode/1, type]

  def loaders(:string, {:parameterized, Ecto.Enum, %{on_load: on_load}}),
    do: [fn val -> enum_decode(on_load, val) end, :any]

  def loaders(_, type), do: [type]

  defp int_decode(int) when is_binary(int), do: {:ok, String.to_integer(int)}
  defp int_decode(int), do: {:ok, int}

  defp decimal_decode(dec) do
    with {parsed, _rem} <- Decimal.parse(dec) do
      {:ok, parsed}
    else
      err -> err
    end
  end

  defp time_decode(time), do: Time.from_iso8601(time)

  defp datetime_decode({date, time}) do
    with {:ok, date} <- Date.from_erl(date),
         {:ok, time} <- Time.from_erl(time) do
      DateTime.new(date, time)
    else
      err -> err
    end
  end

  defp float_decode(float) when is_float(float), do: float

  defp float_decode(float) do
    {val, _} = Float.parse(float)
    {:ok, val}
  end

  defp date_decode(date), do: Date.from_iso8601(date)

  defp uuid_decode(uuid) do
    with {:ok, raw} <- Base.decode16(uuid) do
      Ecto.UUID.load(raw)
    else
      err -> err
    end
  end

  defp enum_decode(on_load, val) do
    {:ok, on_load[val]}
  end
end
