defmodule HttpRepoTest do
  use ExUnit.Case

  import Ecto.Query
  alias Snowflex.AllTestTypes
  alias Snowflex.Post
  alias Snowflex.TestPost
  alias Snowflex.TestUser
  alias Snowflex.User

  @moduletag :integration

  setup do
    start_supervised(Http)
    :ok
  end

  setup context do
    if context[:mutation_test] do
      # Create tables if they don't exist
      Http.query("""
      CREATE TABLE IF NOT EXISTS snowflex_test_users (
        id INTEGER PRIMARY KEY,
        name VARCHAR,
        email VARCHAR,
        created_at TIMESTAMP_NTZ,
        updated_at TIMESTAMP_NTZ
      )
      """)

      Http.query("""
      CREATE TABLE IF NOT EXISTS snowflex_test_posts (
        id INTEGER PRIMARY KEY,
        user_id INTEGER,
        title VARCHAR,
        body VARCHAR,
        published_at TIMESTAMP_NTZ,
        created_at TIMESTAMP_NTZ,
        updated_at TIMESTAMP_NTZ
      )
      """)

      # Clean up any existing data
      Http.query("DELETE FROM snowflex_test_posts")
      Http.query("DELETE FROM snowflex_test_users")
    end

    :ok
  end

  describe "all/2" do
    test "can fetch all records" do
      query = from(u in User, limit: 10, order_by: [desc: :id])
      result = Http.all(query, my_key: "my_value")

      assert is_list(result)
      assert length(result) <= 10
    end

    test "can fetch all records with options" do
      query = from(u in User, limit: 5, order_by: [desc: :id])
      result = Http.all(query, timeout: 30_000)

      assert is_list(result)
      assert length(result) <= 5
    end
  end

  describe "get/3" do
    test "can get a record by id" do
      # First get a record to use its ID
      record =
        User
        |> limit(1)
        |> order_by(desc: :id)
        |> Http.one()

      assert record

      # Now try to get the same record by ID
      result = Http.get(User, record.id)
      assert result
      assert result.id == record.id
    end
  end

  describe "get_by/3" do
    test "can get a record by attributes" do
      # First get a record to use its attributes
      record =
        User
        |> limit(1)
        |> order_by(desc: :id)
        |> Http.one()

      assert record

      # Now try to get the same record by attributes
      result = Http.get_by(User, id: record.id)
      assert result
      assert result.id == record.id
    end
  end

  describe "one/2" do
    test "can fetch one record" do
      result =
        User
        |> limit(1)
        |> order_by(desc: :id)
        |> Http.one()

      assert result
      assert is_struct(result, User)
    end

    test "returns nil when no record exists" do
      result =
        User
        |> where([u], u.id == -999)
        |> Http.one()

      assert is_nil(result)
    end
  end

  describe "one!/2" do
    test "can fetch one record" do
      result =
        User
        |> limit(1)
        |> order_by(desc: :id)
        |> Http.one!()

      assert result
      assert is_struct(result, User)
    end

    test "raises when no record exists" do
      assert_raise Ecto.NoResultsError, fn ->
        User
        |> where([u], u.id == -999)
        |> Http.one!()
      end
    end
  end

  describe "aggregate/3" do
    test "can count records" do
      count =
        User
        |> limit(100)
        |> Http.aggregate(:count)

      assert is_integer(count)
      assert count <= 100
    end

    test "can count distinct values" do
      distinct_count =
        User
        |> limit(100)
        |> Http.aggregate(:count, :id)

      assert is_integer(distinct_count)
      assert distinct_count <= 100
    end

    test "can use other aggregate functions" do
      max_result =
        User
        |> limit(100)
        |> Http.aggregate(:max, :id)

      assert max_result
    end
  end

  describe "exists?/2" do
    test "returns true when records exist" do
      exists =
        User
        |> limit(1)
        |> order_by(desc: :id)
        |> Http.exists?()

      assert exists == true
    end

    test "returns false when no records exist" do
      exists =
        User
        |> where([u], u.id == -999)
        |> Http.exists?()

      assert exists == false
    end
  end

  describe "preload/3" do
    test "can preload associations" do
      # Get some user records
      users =
        User
        |> limit(5)
        |> Http.all()

      # Preload their posts
      result = Http.preload(users, :posts)
      assert is_list(result)
      assert length(result) == length(users)
    end
  end

  describe "stream/2" do
    # streaming shouldd work WITHOUT a transaction
    test "can stream records" do
      results =
        User
        |> limit(10)
        |> Http.stream()
        |> Enum.to_list()

      assert is_list(results)
      assert length(results) <= 10
      assert Enum.all?(results, &is_struct(&1, User))
    end

    test "can stream with chunk size" do
      results =
        User
        |> limit(20)
        |> Http.stream(max_rows: 5)
        |> Enum.to_list()

      assert is_list(results)
      assert length(results) <= 20
    end
  end

  describe "query/2" do
    test "can execute raw SQL query" do
      result = Http.query("SELECT * FROM users LIMIT 5")

      assert {:ok, %{num_rows: count, rows: rows}} = result
      assert count <= 5
      assert is_list(rows)
    end
  end

  describe "to_sql/2" do
    test "can convert query to SQL" do
      {sql, params} =
        User
        |> limit(1)
        |> order_by(desc: :id)
        |> then(fn query ->
          Http.to_sql(:all, query)
        end)

      assert is_binary(sql)
      assert is_list(params)
    end
  end

  describe "load/2" do
    test "can load structs from query results" do
      # Get some raw data
      {:ok, %{rows: rows, columns: columns}} =
        Http.query("SELECT * FROM users LIMIT 1")

      # Create a map from the results
      row_map =
        columns
        |> Enum.zip(List.first(rows) || [])
        |> Enum.into(%{})

      # Load into a struct
      loaded = Http.load(User, row_map)

      assert is_struct(loaded, User)
    end
  end

  describe "relationships" do
    test "can query related records" do
      # Get a user with posts
      user =
        User
        |> limit(1)
        |> Http.one()

      assert user

      # Find posts for this user
      posts =
        Post
        |> where([p], p.user_id == ^user.id)
        |> Http.all()

      # This just verifies the query runs successfully
      assert is_list(posts)
    end
  end

  describe "AllTestTypes encoding/decoding" do
    test "can query all data types" do
      result =
        AllTestTypes
        |> limit(1)
        |> Http.all()

      assert is_list(result)
    end

    test "can handle decimal values" do
      result =
        AllTestTypes
        |> select([t], t.col_number)
        |> limit(1)
        |> Http.one()

      assert is_nil(result) || is_number(result) || Decimal.is_decimal(result)
    end

    test "can handle integer values" do
      result =
        AllTestTypes
        |> select([t], t.col_int)
        |> limit(1)
        |> Http.one()

      assert is_nil(result) || is_integer(result)
    end

    test "can handle float values" do
      result =
        AllTestTypes
        |> select([t], t.col_float)
        |> limit(1)
        |> Http.one()

      assert is_nil(result) || is_float(result)
    end

    test "can handle boolean values" do
      result =
        AllTestTypes
        |> select([t], t.col_boolean)
        |> limit(1)
        |> Http.one()

      assert is_nil(result) || is_boolean(result)
    end

    test "can handle string values" do
      result =
        AllTestTypes
        |> select([t], t.col_varchar)
        |> limit(1)
        |> Http.one()

      assert is_nil(result) || is_binary(result)
    end

    test "can handle binary values" do
      result =
        AllTestTypes
        |> select([t], t.col_binary)
        |> limit(1)
        |> Http.one()

      assert is_nil(result) || is_binary(result)
    end

    test "can handle date values" do
      result =
        AllTestTypes
        |> select([t], t.col_date)
        |> limit(1)
        |> Http.one()

      assert is_nil(result) || is_struct(result, Date)
    end

    test "can handle time values" do
      result =
        AllTestTypes
        |> select([t], t.col_time)
        |> limit(1)
        |> Http.one()

      assert is_nil(result) || is_struct(result, Time)
    end

    test "can handle timestamp values" do
      result =
        AllTestTypes
        |> select([t], t.col_timestamp_ltz)
        |> limit(1)
        |> Http.one()

      assert is_nil(result) || is_struct(result, DateTime)

      result =
        AllTestTypes
        |> select([t], t.col_timestamp_ntz)
        |> limit(1)
        |> Http.one()

      assert is_nil(result) || is_struct(result, NaiveDateTime)

      result =
        AllTestTypes
        |> select([t], t.col_timestamp_tz)
        |> limit(1)
        |> Http.one()

      assert is_nil(result) || is_struct(result, DateTime)
    end

    test "can handle map values" do
      result =
        AllTestTypes
        |> select([t], t.col_variant)
        |> limit(1)
        |> Http.one()

      assert is_nil(result) || is_map(result)

      result =
        AllTestTypes
        |> select([t], t.col_object)
        |> limit(1)
        |> Http.one()

      assert is_nil(result) || is_map(result)

      result =
        AllTestTypes
        |> select([t], t.col_array)
        |> limit(1)
        |> Http.one()

      assert is_nil(result) || is_map(result)
    end

    test "can handle geography values" do
      result =
        AllTestTypes
        |> select([t], t.col_geography)
        |> limit(1)
        |> Http.one()

      assert is_nil(result) || is_binary(result)
    end
  end

  describe "insert/2" do
    @describetag :mutation_test
    test "can insert data into the test users table" do
      # Generate a timestamp for testing
      timestamp = DateTime.utc_now(:second)

      # Create a new user record
      user = %TestUser{
        id: 1,
        name: "Test User",
        email: "test@example.com",
        created_at: timestamp,
        updated_at: timestamp
      }

      # Insert the record
      result = Http.insert(user)

      # Verify the result
      assert {:ok, inserted_user} = result
      assert inserted_user.id == 1
      assert inserted_user.name == "Test User"
      assert inserted_user.email == "test@example.com"

      # Query to verify the data was inserted
      {:ok, %{num_rows: count, rows: rows}} =
        Http.query("SELECT * FROM snowflex_test_users WHERE id = 1")

      assert count == 1
      assert List.first(rows)
    end

    test "can insert_all data into the test users table" do
      # Generate a timestamp for testing
      timestamp = DateTime.utc_now(:second)

      # Prepare multiple user records
      users = [
        %{
          id: 1,
          name: "User One",
          email: "user1@example.com",
          created_at: timestamp,
          updated_at: timestamp
        },
        %{
          id: 2,
          name: "User Two",
          email: "user2@example.com",
          created_at: timestamp,
          updated_at: timestamp
        }
      ]

      # Insert all records
      result = Http.insert_all(TestUser, users)

      # Verify the result
      assert {count, nil} = result
      assert count == 2

      # Query to verify the data was inserted
      {:ok, %{num_rows: count, rows: rows}} =
        Http.query("SELECT COUNT(*) FROM snowflex_test_users")

      assert count == 1
      assert List.first(rows) == [2]
    end
  end

  describe "update/2" do
    test "can update data in the test users table" do
      # Use a truncated timestamp to avoid precision issues with Snowflake
      timestamp = DateTime.utc_now(:second)

      # Insert initial test data
      {:ok, _} =
        Http.query("""
        INSERT INTO snowflex_test_users (id, name, email, created_at, updated_at)
        VALUES (1, 'Original Name_to_update', 'original@example.com', '#{timestamp}', '#{timestamp}')
        """)

      # Get the record
      query = from(u in TestUser, where: u.name == "Original Name_to_update")
      original_user = Http.one(query)
      assert original_user.name == "Original Name_to_update"

      # Update the record
      updated_timestamp = DateTime.utc_now(:second)

      changeset =
        TestUser.changeset(original_user, %{
          name: "Updated Name",
          updated_at: updated_timestamp
        })

      assert {:ok, _update_result} = Http.update(changeset)
    end
  end

  describe "delete/2" do
    @describetag :mutation_test

    test "can delete data from the test users table" do
      # Generate a timestamp for testing
      timestamp = DateTime.utc_now(:second)

      # Insert test data
      {:ok, _} =
        Http.query("""
        INSERT INTO snowflex_test_users (id, name, email, created_at, updated_at)
        VALUES (1, 'Test User', 'test@example.com', '#{timestamp}', '#{timestamp}')
        """)

      # Get the record
      query = from(u in TestUser, where: u.id == 1)
      user = Http.one(query)
      assert user.id == 1

      # Delete the record
      delete_result = Http.delete(user)

      # Verify the delete result
      assert {:ok, deleted_user} = delete_result
      assert deleted_user.id == 1

      # Query to verify the data was deleted
      {:ok, %{rows: [[count]]}} =
        Http.query("SELECT COUNT(*) FROM snowflex_test_users WHERE id = 1")

      assert count == 0
    end

    test "can delete_all data from the test users table" do
      # Generate a timestamp for testing
      timestamp = DateTime.utc_now(:second)

      # Insert multiple test records
      {:ok, _} =
        Http.query("""
        INSERT INTO snowflex_test_users (id, name, email, created_at, updated_at)
        VALUES
          (1, 'User One', 'user1@example.com', '#{timestamp}', '#{timestamp}'),
          (2, 'User Two', 'user2@example.com', '#{timestamp}', '#{timestamp}'),
          (3, 'User Three', 'user3@example.com', '#{timestamp}', '#{timestamp}')
        """)

      # Delete all records with id > 1
      delete_result =
        TestUser
        |> where([u], u.id > 1)
        |> Http.delete_all()

      # Verify the delete result
      assert {count, nil} = delete_result
      assert count == 2

      # Query to verify only the expected data was deleted
      {:ok, %{rows: [[remaining_count]]}} =
        Http.query("SELECT COUNT(*) FROM snowflex_test_users")

      assert remaining_count == 1

      {:ok, %{rows: [[remaining_id]]}} =
        Http.query("SELECT id FROM snowflex_test_users")

      assert remaining_id == 1
    end
  end

  describe "relationships with test tables" do
    @tag :mutation_test
    test "can establish and query relationships between test tables" do
      # Generate a timestamp for testing
      timestamp = DateTime.utc_now(:second)

      # Insert test user
      {:ok, _} =
        Http.query("""
        INSERT INTO snowflex_test_users (id, name, email, created_at, updated_at)
        VALUES (1, 'Test User', 'test@example.com', '#{timestamp}', '#{timestamp}')
        """)

      # Insert test posts for the user
      {:ok, _} =
        Http.query("""
        INSERT INTO snowflex_test_posts (id, user_id, title, body, created_at, updated_at)
        VALUES
          (1, 1, 'First Post', 'This is the first post', '#{timestamp}', '#{timestamp}'),
          (2, 1, 'Second Post', 'This is the second post', '#{timestamp}', '#{timestamp}')
        """)

      # Query the user
      user = Http.get(TestUser, 1)
      assert user.id == 1
      assert user.name == "Test User"

      # Query the user's posts
      posts =
        TestPost
        |> where([p], p.user_id == 1)
        |> Http.all()

      assert length(posts) == 2
      assert Enum.at(posts, 0).title == "First Post"
      assert Enum.at(posts, 1).title == "Second Post"

      # Test preloading (we need to define association in the schemas)
      user_with_posts = user |> Http.preload(:posts)
      assert length(user_with_posts.posts) == 2
      assert Enum.at(user_with_posts.posts, 0).title == "First Post"
      assert Enum.at(user_with_posts.posts, 1).title == "Second Post"
    end
  end

  describe "semi-structured data queries" do
    @describetag :mutation_test
    test "can query variant data using json path" do
      # Create a test table with variant data
      Http.query("""
      CREATE TABLE IF NOT EXISTS snowflex_test_variants (
        id INTEGER PRIMARY KEY,
        data VARIANT,
        created_at TIMESTAMP_NTZ
      )
      """)

      # Clean up any existing data
      Http.query("DELETE FROM snowflex_test_variants")

      # Insert test data
      timestamp = DateTime.utc_now(:second)

      {:ok, _} =
        Http.query("""
        INSERT INTO snowflex_test_variants (id, data, created_at)
        SELECT 1, PARSE_JSON('{"foo": {"bar": 100}}'), '#{timestamp}';
        """)

      {:ok, _} =
        Http.query("""
        INSERT INTO snowflex_test_variants (id, data, created_at)
        SELECT 2, PARSE_JSON('{"foo": {"bar": 50}}'), '#{timestamp}';
        """)

      {:ok, _} =
        Http.query("""
        INSERT INTO snowflex_test_variants (id, data, created_at)
        SELECT 3, PARSE_JSON('{"foo": {"bar": 200}}'), '#{timestamp}';
        """)

      # Query using json path
      query =
        from(v in "snowflex_test_variants",
          where: v.data["foo"]["bar"] > 90,
          select: v.id
        )

      results = Http.all(query)
      assert length(results) == 2
      assert Enum.sort(results) == [1, 3]

      # Clean up
      Http.query("DROP TABLE IF EXISTS snowflex_test_variants")
    end

    test "can query object data using dot notation" do
      # Create a test table with object data
      Http.query("""
      CREATE TABLE IF NOT EXISTS snowflex_test_objects (
        id INTEGER PRIMARY KEY,
        data OBJECT,
        created_at TIMESTAMP_NTZ
      )
      """)

      # Clean up any existing data
      Http.query("DELETE FROM snowflex_test_objects")

      # Insert test data
      timestamp = DateTime.utc_now(:second)

      {:ok, _} =
        Http.query("""
        INSERT INTO snowflex_test_objects (id, data, created_at)
        SELECT 1, PARSE_JSON('{"name": "John", "age": 30}'), '#{timestamp}'
        UNION ALL
        SELECT 2, PARSE_JSON('{"name": "Jane", "age": 25}'), '#{timestamp}'
        UNION ALL
        SELECT 3, PARSE_JSON('{"name": "Bob", "age": 35}'), '#{timestamp}';
        """)

      # Query using dot notation
      query =
        from(o in "snowflex_test_objects",
          where: o.data["age"] > 30,
          select: o.id
        )

      results = Http.all(query)
      assert length(results) == 1
      assert hd(results) == 3

      # Clean up
      Http.query("DROP TABLE IF EXISTS snowflex_test_objects")
    end
  end
end
