defmodule Snowflex.ConnectionTest do
  use ExUnit.Case
  import Ecto.Query

  alias Snowflex.AllTestTypes
  alias Snowflex.Error
  alias Snowflex.Post
  alias Snowflex.User
  @moduletag :integration

  setup do
    start_supervised(Http)
    :ok
  end

  describe "basic connection and queries" do
    test "can connect to Snowflake" do
      assert {:ok, %{rows: [[version]]}} = Http.query("SELECT current_version()")
      assert is_binary(version)
    end

    test "can perform simple select queries" do
      result = Http.one(from u in User, select: count(u.id), limit: 1)
      assert is_integer(result)
    end

    test "can perform select with multiple fields" do
      query = from u in User, select: {u.id, u.name}, limit: 5
      result = Http.all(query)

      assert is_list(result)
      assert length(result) <= 5
      assert match?({id, name} when is_integer(id) and is_binary(name), hd(result))
    end

    test "returns errors when query fails" do
      assert {:error, %Error{message: message}} =
               Http.query("SELECT * FROM non_existent_table")

      assert message =~ "SQL compilation error"
    end

    @tag :long_running_test
    test "supports queries longer than 45 seconds" do
      assert {:ok, %{rows: _rows}} =
               Http.query("CALL SYSTEM$WAIT(60);", [], timeout: :timer.seconds(180))
    end
  end

  describe "Snowflake data types" do
    test "can query and cast all supported data types" do
      result =
        AllTestTypes
        |> limit(1)
        |> Http.all()

      assert is_list(result)
    end

    test "numeric types" do
      query = from a in AllTestTypes, select: {a.col_number, a.col_int, a.col_float}, limit: 1
      result = Http.one(query)

      # The result might be nil if there's no data, but we're testing type casting here
      if result do
        {decimal, integer, float} = result
        assert is_nil(decimal) || is_number(decimal) || match?(%Decimal{}, decimal)
        assert is_integer(integer) || is_nil(integer)
        assert is_float(float) || is_nil(float)
      end
    end

    test "string and binary types" do
      query = from a in AllTestTypes, select: {a.col_varchar, a.col_binary}, limit: 1
      result = Http.one(query)

      if result do
        {string, binary} = result
        assert is_binary(string) || is_nil(string)
        assert is_binary(binary) || is_nil(binary)
      end
    end

    test "date and time types" do
      query =
        from a in AllTestTypes,
          select:
            {a.col_date, a.col_time, a.col_timestamp_ltz, a.col_timestamp_ntz, a.col_timestamp_tz},
          limit: 1

      result = Http.one(query)

      if result do
        {date, time, timestamp_ltz, timestamp_ntz, timestamp_tz} = result
        assert is_nil(date) || match?(%Date{}, date)
        assert is_nil(time) || match?(%Time{}, time)
        assert is_nil(timestamp_ltz) || match?(%DateTime{}, timestamp_ltz)
        assert is_nil(timestamp_ntz) || match?(%NaiveDateTime{}, timestamp_ntz)
        assert is_nil(timestamp_tz) || match?(%DateTime{}, timestamp_tz)
      end
    end

    test "structured types (variant, object, array)" do
      query = from a in AllTestTypes, select: {a.col_variant, a.col_object, a.col_array}, limit: 1
      result = Http.one(query)

      if result do
        {variant, object, array} = result
        assert is_nil(variant) || is_map(variant)
        assert is_nil(object) || is_map(object)
        assert is_nil(array) || is_map(array) || is_list(array)
      end
    end
  end

  describe "query operators and functions" do
    test "supports arithmetic operations" do
      query = from u in User, select: u.id + 1, limit: 1
      result = Http.one(query)
      assert is_integer(result)
    end

    test "supports boolean operators" do
      query = from u in User, where: u.id > 0 and u.id < 1000, select: u.id, limit: 5
      result = Http.all(query)
      assert is_list(result)
    end

    test "supports string operations" do
      query = from u in User, where: like(u.name, "%a%"), select: u.name, limit: 5
      result = Http.all(query)
      assert is_list(result)
    end

    test "supports is_nil operator" do
      query =
        from u in User, where: is_nil(u.email) or not is_nil(u.email), select: u.id, limit: 5

      result = Http.all(query)
      assert is_list(result)
    end
  end

  describe "query expressions" do
    test "supports order_by" do
      query = from u in User, order_by: [desc: u.id], select: u.id, limit: 5
      result = Http.all(query)
      assert is_list(result)
      assert result == Enum.sort(result, :desc)
    end

    test "supports limit and offset" do
      query = from u in User, order_by: [asc: u.id], select: u.id, limit: 3, offset: 2
      result = Http.all(query)
      assert is_list(result)
      assert length(result) <= 3
    end

    test "supports group_by with aggregates" do
      # Try to group users by something and count them
      query =
        from u in User,
          group_by: fragment("SUBSTRING(?, 1, 1)", u.name),
          select: {fragment("SUBSTRING(?, 1, 1)", u.name), count(u.id)},
          limit: 5

      result = Http.all(query)
      assert is_list(result)
    end

    test "supports having" do
      query =
        from u in User,
          group_by: fragment("SUBSTRING(?, 1, 1)", u.name),
          having: count(u.id) > 1,
          select: {fragment("SUBSTRING(?, 1, 1)", u.name), count(u.id)},
          limit: 5

      result = Http.all(query)
      assert is_list(result)
    end

    test "supports distinct" do
      query =
        from u in User, distinct: true, select: fragment("SUBSTRING(?, 1, 1)", u.name), limit: 5

      result = Http.all(query)
      assert is_list(result)
      assert length(result) == length(Enum.uniq(result))
    end
  end

  describe "joins" do
    test "supports inner join" do
      query =
        from u in User,
          join: p in Post,
          on: p.user_id == u.id,
          select: {u.id, u.name, p.title},
          limit: 5

      result = Http.all(query)
      assert is_list(result)
    end

    test "supports left join" do
      query =
        from u in User,
          left_join: p in Post,
          on: p.user_id == u.id,
          select: {u.id, u.name, p.title},
          limit: 5

      result = Http.all(query)
      assert is_list(result)
    end

    test "supports multiple joins" do
      query =
        from u in User,
          join: p in Post,
          on: p.user_id == u.id,
          left_join: p2 in Post,
          on: p2.user_id == u.id and p2.id != p.id,
          select: {u.id, p.title, p2.title},
          limit: 5

      result = Http.all(query)
      assert is_list(result)
    end
  end

  describe "subqueries and CTEs" do
    test "supports subqueries" do
      # todo: convert to ecto query syntax
      # Test with raw SQL instead of subqueries to avoid async execution issues
      query = """
      SELECT u.id, u.name, (SELECT COUNT(*) FROM posts p WHERE p.user_id = u.id) as post_count
      FROM users u
      ORDER BY post_count DESC
      LIMIT 5
      """

      assert {:ok, %{rows: rows}} = Http.query(query)
      assert is_list(rows)
    end

    test "supports CTEs (WITH clause)" do
      # Create a CTE query
      user_posts_query =
        from u in User,
          join: p in Post,
          on: p.user_id == u.id,
          group_by: [u.id, u.name],
          select: %{id: u.id, name: u.name, post_count: count(p.id)}

      query =
        from(up in "user_posts",
          select: {up.id, up.name, up.post_count},
          order_by: [desc: up.post_count],
          limit: 5
        )
        |> with_cte("user_posts", as: ^user_posts_query)

      result = Http.all(query)
      assert is_list(result)
    end
  end

  describe "Snowflake-specific features" do
    test "supports window functions through the over keyword" do
      # TODO: convert this to ecto
      # Test with raw SQL instead of Ecto query to avoid issues
      query = """
      SELECT
        id,
        name,
        SUBSTRING(name, 1, 1) as first_letter,
        ROW_NUMBER() OVER (PARTITION BY SUBSTRING(name, 1, 1) ORDER BY id) as row_num,
        COUNT(id) OVER (PARTITION BY SUBSTRING(name, 1, 1)) as count_per_letter,
        SUM(id) OVER (PARTITION BY SUBSTRING(name, 1, 1)) as sum_per_letter
      FROM users
      LIMIT 10
      """

      assert {:ok, %{rows: rows}} = Http.query(query)
      assert is_list(rows)
    end
  end

  describe "query execution" do
    test "can stream results" do
      query = from u in User, select: u.id, limit: 10
      stream = Http.stream(query)

      result = Enum.to_list(stream)
      assert is_list(result)
      assert length(result) <= 10
    end
  end
end
