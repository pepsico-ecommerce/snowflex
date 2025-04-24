defmodule TestSchema2 do
  @moduledoc """
  Test schema representing comments with a belongs_to association to TestSchema.
  """
  use Ecto.Schema

  schema "schema2" do
    belongs_to(:post, TestSchema,
      references: :x,
      foreign_key: :z
    )
  end
end
