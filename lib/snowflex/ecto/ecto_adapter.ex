defmodule Snowflex.EctoAdapter do
  use Ecto.Adapters.SQL,
    driver: :snowflex

  @impl true
  def supports_ddl_transaction?, do: false

  @impl true
  def lock_for_migrations(_meta, _opts, _fun) do
    raise "Migrations are not supported"
  end
end
