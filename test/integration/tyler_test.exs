defmodule TylerTest do
  use ExUnit.Case

  import Ecto.Query

  @moduletag :integration

  defmodule SnowflexTestMapSchema do
    use Ecto.Schema

    @primary_key {:id, :string, []}
    schema "SNOWFLEX_TEST_MAP_SCHEMA" do
      field :tags, :map
    end
  end

  setup_all do
    start_supervised(Http)
    :ok
  end

  setup do
    Http.query("""
    CREATE TABLE IF NOT EXISTS SNOWFLEX_TEST_MAP_SCHEMA (
      id VARCHAR NOT NULL,
      tags VARIANT
    );
    """)

    on_exit(fn ->
      Http.query("DROP TABLE IF EXISTS SNOWFLEX_TEST_MAP_SCHEMA;")
    end)
  end

  test "write map values to Snowflake" do
    id = "row-#{:erlang.unique_integer([:positive])}}"
    tags = %{"a" => 1, "b" => 2, "c" => %{"d" => 3}}

    {:ok, _} = Http.insert(%SnowflexTestMapSchema{id: id, tags: tags})

    assert %Snowflex.Result{rows: [written_tags]} =
             Http.query!("SELECT tags FROM SNOWFLEX_TEST_MAP_SCHEMA WHERE id = '#{id}';")

    assert Jason.decode!(written_tags) == tags
  end
end
