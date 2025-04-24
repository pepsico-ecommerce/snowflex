defmodule TestSchema do
  @moduledoc """
  Test schema for Ecto adapter testing with associations.
  """
  use Ecto.Schema

  schema "schema" do
    field(:x, :integer)
    field(:y, :integer)
    field(:z, :integer)
    field(:meta, :map)

    has_many(:comments, TestSchema2,
      references: :x,
      foreign_key: :z
    )

    has_one(:permalink, TestSchema3,
      references: :y,
      foreign_key: :id
    )
  end
end
