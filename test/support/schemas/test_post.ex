defmodule Snowflex.TestPost do
  @moduledoc false
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :integer, []}
  schema "snowflex_test_posts" do
    field(:title, :string)
    field(:body, :string)
    field(:published_at, :utc_datetime)
    field(:created_at, :utc_datetime)
    field(:updated_at, :utc_datetime)

    belongs_to(:user, Snowflex.TestUser)
  end

  @spec changeset(Ecto.Schema.t(), map()) :: Ecto.Changeset.t()
  def changeset(post, attrs) do
    post
    |> cast(attrs, [:id, :user_id, :title, :body, :published_at, :created_at, :updated_at])
    |> validate_required([:id, :title, :body])
  end
end
