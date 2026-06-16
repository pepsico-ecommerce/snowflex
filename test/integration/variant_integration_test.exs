defmodule Snowflex.VariantIntegrationTest do
  use ExUnit.Case, async: false

  alias Ecto.Changeset

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

  defmodule SnowflexTestMixedSchema do
    use Ecto.Schema

    @primary_key {:id, :string, []}
    schema "SNOWFLEX_TEST_MIXED_SCHEMA" do
      field :label, :string
      field :meta, :map
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

  setup_all do
    Http.query!("""
    CREATE TABLE IF NOT EXISTS SNOWFLEX_TEST_MIXED_SCHEMA (
      id VARCHAR NOT NULL,
      label VARCHAR,
      meta VARIANT
    );
    """)

    :ok
  end

  setup do
    id = "row-#{:erlang.unique_integer([:positive])}"

    Http.query!("DELETE FROM SNOWFLEX_TEST_MAP_SCHEMA WHERE id = '#{id}';")
    Http.query!("DELETE FROM SNOWFLEX_TEST_ARRAY_SCHEMA WHERE id = '#{id}';")
    Http.query!("DELETE FROM SNOWFLEX_TEST_MIXED_SCHEMA WHERE id = '#{id}';")

    %{id: id}
  end

  describe "map values" do
    test "write map values to Snowflake", %{id: id} do
      tags = %{"a" => 1, "b" => 2, "c" => %{"d" => 3}}

      Http.insert!(%SnowflexTestMapSchema{id: id, tags: tags})

      assert %Snowflex.Result{rows: [[written_tags]]} =
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

    test "read array values from Snowflake", %{id: id} do
      names = ["Alice", "Bob"]
      ages = [7, 45, 99]
      things = [%{"color" => "red"}, %{"amount" => 10.5}]

      Http.query!("""
      INSERT INTO SNOWFLEX_TEST_ARRAY_SCHEMA (id, names, ages, things)
      SELECT '#{id}',
             PARSE_JSON('#{Jason.encode!(names)}'),
             PARSE_JSON('#{Jason.encode!(ages)}'),
             PARSE_JSON('#{Jason.encode!(things)}');
      """)

      assert %SnowflexTestArraySchema{names: ^names, ages: ^ages, things: ^things} =
               Http.get(SnowflexTestArraySchema, id)
    end
  end

  describe "update variant fields (corruption-fix proof)" do
    test "update :map field to new map round-trips as parsed JSON, not a quoted string", %{
      id: id
    } do
      original = %{"version" => 1}
      updated = %{"version" => 2, "extra" => "data"}

      row = %SnowflexTestMapSchema{id: id, tags: original}
      Http.insert!(row)

      changeset = Changeset.change(row, tags: updated)
      Http.update!(changeset)

      assert %SnowflexTestMapSchema{tags: ^updated} = Http.get(SnowflexTestMapSchema, id)
    end

    test "update {:array, _} field to new list round-trips correctly", %{id: id} do
      original_names = ["Alice"]
      updated_names = ["Alice", "Bob", "Carol"]

      row = %SnowflexTestArraySchema{id: id, names: original_names, ages: [], things: []}
      Http.insert!(row)

      changeset = Changeset.change(row, names: updated_names)
      Http.update!(changeset)

      assert %SnowflexTestArraySchema{names: ^updated_names} =
               Http.get(SnowflexTestArraySchema, id)
    end

    test "update a variant col and a plain field together in one changeset", %{id: id} do
      original_meta = %{"v" => 1}
      updated_meta = %{"v" => 2}
      original_label = "before"
      updated_label = "after"

      row = %SnowflexTestMixedSchema{id: id, label: original_label, meta: original_meta}
      Http.insert!(row)

      changeset = Changeset.change(row, label: updated_label, meta: updated_meta)
      Http.update!(changeset)

      assert %SnowflexTestMixedSchema{label: ^updated_label, meta: ^updated_meta} =
               Http.get(SnowflexTestMixedSchema, id)
    end

    test "update a variant field to nil stores SQL NULL and reads back nil", %{id: id} do
      row = %SnowflexTestMapSchema{id: id, tags: %{"key" => "value"}}
      Http.insert!(row)

      changeset = Changeset.change(row, tags: nil)
      Http.update!(changeset)

      assert %SnowflexTestMapSchema{tags: nil} = Http.get(SnowflexTestMapSchema, id)
    end

    test "delete by id returns nil from Http.get", %{id: id} do
      row = %SnowflexTestMapSchema{id: id, tags: %{"x" => 1}}
      Http.insert!(row)

      Http.delete!(row)

      assert nil == Http.get(SnowflexTestMapSchema, id)
    end
  end

  describe "update_all variant fields (PARSE_JSON wrap)" do
    test "update_all on a :map column stores a parsed object, not a quoted JSON string", %{
      id: id
    } do
      import Ecto.Query

      original = %{"version" => 1}
      new_tags = %{"version" => 2, "extra" => "data"}

      Http.insert!(%SnowflexTestMapSchema{id: id, tags: original})

      Http.update_all(
        from(r in SnowflexTestMapSchema, where: r.id == ^id),
        set: [tags: new_tags]
      )

      # After update_all the row must round-trip as a parsed map, not a quoted
      # JSON string stored as a scalar VARIANT.
      assert %SnowflexTestMapSchema{tags: ^new_tags} = Http.get(SnowflexTestMapSchema, id)
    end

    test "update_all on a {:array, _} column stores a parsed array, not a quoted JSON string",
         %{id: id} do
      import Ecto.Query

      original_names = ["Alice"]
      new_names = ["Alice", "Bob", "Carol"]

      Http.insert!(%SnowflexTestArraySchema{id: id, names: original_names, ages: [], things: []})

      Http.update_all(
        from(r in SnowflexTestArraySchema, where: r.id == ^id),
        set: [names: new_names]
      )

      assert %SnowflexTestArraySchema{names: ^new_names} = Http.get(SnowflexTestArraySchema, id)
    end
  end
end
