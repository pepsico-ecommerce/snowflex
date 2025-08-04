defmodule Snowflex.User do
  @moduledoc """
  Test schema representing a user for testing Snowflex adapter.
  """
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :integer, read_after_writes: true}
  schema "users" do
    field(:name, :string)
    field(:email, :string)
    field(:created_at, :utc_datetime)
    field(:updated_at, :utc_datetime)

    has_many(:posts, Snowflex.Post)
  end

  @spec changeset(Ecto.Schema.t(), map()) :: Ecto.Changeset.t()
  def changeset(user, attrs) do
    user
    |> cast(attrs, [:name, :email])
    |> validate_required([:name, :email])
  end
end
