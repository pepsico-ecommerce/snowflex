defmodule Snowflex.QueryTest do
  use ExUnit.Case

  alias Snowflex.Query

  describe "create!/1" do
    test "should create a new Snowflex.Query with no params" do
      assert %Query{query_string: "my string", params: nil} =
               Query.create!(%{query_string: "my string"})
    end

    test "should create a Snowflex.Query with params" do
      assert %Query{query_string: "my string", params: [{{:sql_varchar, 250}, "hi"}]} =
               Query.create!(%{query_string: "my string", params: ["hi"]})
    end

    test "should raise an error when missing params" do
      assert_raise ArgumentError, "must provide :query_string to build Query", fn ->
        Query.create!(%{params: ["hi"]})
      end
    end

    test "should raise an error with an unsupported param type" do
      assert_raise ArgumentError, "unsupported parameter type given", fn ->
        Query.create!(%{query_string: "my string", params: [nil]})
      end
    end
  end

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
      assert {:error, "must provide :query_string to build Query"} =
               Query.create(%{params: ["hi"]})
    end

    test "should return an error with an unsupported param type" do
      assert {:error, "unsupported parameter type given"} =
               Query.create(%{query_string: "my string", params: [nil]})
    end
  end
end
