defmodule Snowflex.QueryTest do
  use ExUnit.Case

  alias Snowflex.Query

  describe "create/1" do
    test "should create a new Snowflex.Query with no params" do
      assert {:ok, %Query{query_string: "my string", params: nil}} =
               Query.create(%{query_string: "my string"})
    end

    test "should create a Snowflex.Query with params" do
      assert {:ok, %Query{query_string: "my string", params: [{{:sql_varchar, 250}, "hi"}]}} =
               Query.create(%{query_string: "my string", params: ["hi"]})
    end

    test "should return an error when missing params" do
      assert {:error, %Ecto.Changeset{}} = Query.create(%{params: ["hi"]})
    end
  end
end
