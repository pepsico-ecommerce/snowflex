defmodule Snowflex.TestUser do
  @moduledoc false
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :integer, []}
  schema "snowflex_test_users" do
    field(:name, :string)
    field(:email, :string)
    field(:created_at, :utc_datetime)
    field(:updated_at, :utc_datetime)
    has_many(:posts, Snowflex.TestPost, references: :id, foreign_key: :user_id)
  end

  @spec changeset(Ecto.Schema.t(), map()) :: Ecto.Changeset.t()
  def changeset(user, attrs) do
    user
    |> cast(attrs, [:id, :name, :email, :created_at, :updated_at])
    |> validate_required([:id, :name, :email])
  end
end
