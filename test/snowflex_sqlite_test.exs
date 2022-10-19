defmodule SnowflexSqliteTest do
  use ExUnit.Case

  alias Snowflex.SQLiteTestRepo, as: Repo

  setup do
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(Repo)
  end

  test "can crete schema" do
    Repo.insert!(%TestSchema{x: 1, y: 2, z: 3})

    assert [%TestSchema{x: 1, y: 2, z: 3}] = Repo.all(TestSchema)
  end
end
