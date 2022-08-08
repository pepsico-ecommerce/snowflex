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
  def loaders(:id, :id), do: [&int_decode/1, :id]
  def loaders(_, type), do: [type]

  defp int_decode(int), do: {:ok, String.to_integer(int)}
end
