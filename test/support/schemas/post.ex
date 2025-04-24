defmodule Snowflex.Post do
  @moduledoc """
  Test schema representing a blog post for testing Snowflex adapter.
  """
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :integer, read_after_writes: true}
  schema "posts" do
    field(:title, :string)
    field(:body, :string)
    field(:published_at, :utc_datetime)
    field(:created_at, :utc_datetime)
    field(:updated_at, :utc_datetime)

    belongs_to(:user, Snowflex.User,
      references: :id,
      foreign_key: :user_id
    )
  end

  @spec changeset(Ecto.Schema.t(), map()) :: Ecto.Changeset.t()
  # Basic changeset for post data
  def changeset(post, attrs) do
    post
    |> cast(attrs, [:user_id, :title, :body, :published_at])
    |> validate_required([:user_id, :title, :body])
  end
end
