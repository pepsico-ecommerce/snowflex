defmodule TestSchema2 do
  use Ecto.Schema

  schema "schema2" do
    belongs_to(:post, TestSchema,
      references: :x,
      foreign_key: :z
    )
  end
end
