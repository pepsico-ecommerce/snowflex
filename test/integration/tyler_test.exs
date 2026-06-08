defmodule TylerTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  defmodule SnowflexTestMapSchema do
    use Ecto.Schema

    @primary_key {:id, :string, []}
    schema "SNOWFLEX_TEST_MAP_SCHEMA" do
      field :tags, :map
    end
  end

  defmodule SnowflexTestArraySchema do
    use Ecto.Schema

    @primary_key {:id, :string, []}
    schema "SNOWFLEX_TEST_ARRAY_SCHEMA" do
      field :names, {:array, :string}
      field :ages, {:array, :integer}
      field :things, {:array, :map}
    end
  end

  setup_all do
    start_supervised(Http)
    :ok
  end

  setup_all do
    Http.query!("""
    CREATE TABLE IF NOT EXISTS SNOWFLEX_TEST_MAP_SCHEMA (
      id VARCHAR NOT NULL,
      tags VARIANT
    );
    """)

    :ok
  end

  setup_all do
    Http.query!("""
    CREATE TABLE IF NOT EXISTS SNOWFLEX_TEST_ARRAY_SCHEMA (
      id VARCHAR NOT NULL,
      names VARIANT,
      ages VARIANT,
      things VARIANT
    );
    """)

    :ok
  end

  setup do
    %{id: "row-#{:erlang.unique_integer([:positive])}}"}
  end

  describe "map values" do
    test "write map values to Snowflake", %{id: id} do
      tags = %{"a" => 1, "b" => 2, "c" => %{"d" => 3}}

      Http.insert!(%SnowflexTestMapSchema{id: id, tags: tags})

      assert %Snowflex.Result{rows: [written_tags]} =
               Http.query!("SELECT tags FROM SNOWFLEX_TEST_MAP_SCHEMA WHERE id = '#{id}';")

      assert Jason.decode!(written_tags) == tags
    end

    test "read map values from Snowflake", %{id: id} do
      tags = %{"a" => 1, "b" => 2, "c" => %{"d" => 3}}

      Http.query!("""
      INSERT INTO SNOWFLEX_TEST_MAP_SCHEMA (id, tags)
      SELECT '#{id}', PARSE_JSON('#{Jason.encode!(tags)}');
      """)

      assert %SnowflexTestMapSchema{tags: ^tags} = Http.get(SnowflexTestMapSchema, id)
    end
  end

  describe "array values" do
    test "write array values to Snowflake", %{id: id} do
      names = ["Alice", "Bob"]
      ages = [7, 45, 99]
      things = [%{"color" => "red"}, %{"amount" => 10.5}]

      Http.insert!(%SnowflexTestArraySchema{id: id, names: names, ages: ages, things: things})

      assert %Snowflex.Result{rows: [[written_names, written_ages, written_things]]} =
               Http.query!(
                 "SELECT names, ages, things FROM SNOWFLEX_TEST_ARRAY_SCHEMA WHERE id = '#{id}';"
               )

      assert Jason.decode!(written_names) == names
      assert Jason.decode!(written_ages) == ages
      assert Jason.decode!(written_things) == things
    end
  end
end
