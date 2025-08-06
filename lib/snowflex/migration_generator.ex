# We can't know these module names ahead of time.
# credo:disable-for-this-file Credo.Check.Warning.UnsafeToAtom
defmodule Snowflex.MigrationGenerator do
  @moduledoc """
  Provides functionality to automatically generate and run migrations based on Ecto schemas or raw table definitions.

  This module is particularly useful when you need to:
  - Test Snowflake schemas using a local database
  - Dynamically switch between Snowflake and local database implementations
  - Generate migrations from both Ecto schemas and raw table definitions

  ## Usage

  First, create a wrapper around your Repo that uses this module:

  ```elixir
  defmodule MyApp.Repo do
    defmacro __using__(_opts) do
      [select_repo(__CALLER__.module), add_schema_helper()]
    end

    defp select_repo(caller_mod) do
      config = Application.get_env(:my_app, caller_mod)

      if config[:use_local_db?] do
        quote do
          use Ecto.Repo,
            otp_app: :my_app,
            adapter: Ecto.Adapters.Postgres

          alias Snowflex.MigrationGenerator
          require Snowflex.MigrationGenerator

          @spec generate_migrations(list(module())) :: :ok
          def generate_migrations(modules) do
            MigrationGenerator.generate_migrations(__MODULE__, modules)
          end
        end
      else
        quote do
          use Ecto.Repo, otp_app: :my_app, adapter: Snowflex
        end
      end
    end

    defp add_schema_helper do
      quote do
        def connection_value(key, default \\ nil) do
          :my_app
          |> Application.get_env(__MODULE__, [])
          |> Keyword.get(key, default)
        end
      end
    end
  end
  ```

  Then you can use it in your application in two ways:

  ### Using Ecto Schemas

  ```elixir
  defmodule MyApp.Data.User do
    use Ecto.Schema
    schema "users" do
      field :name, :string
      field :email, :string
      timestamps()
    end
  end

  # Generate migrations for schemas
  MyApp.Repo.generate_migrations([
    MyApp.Data.User,
    MyApp.Data.Post
  ])
  ```

  We use `generate_migrations` in our `test_helper.exs` file to ensure the database is setup for testing.  Note that this *does* create a new local DB
  for each Snowflake Repo you declare in your application.

  ### Using Raw Table Definitions

  ```elixir
  # Generate migrations for raw table definitions
  MyApp.Repo.generate_migrations([
    {MyApp.Data.CustomTable,
     {"SCHEMA.TABLE_NAME",
      [
        id: :string,
        name: :string,
        created_at: :utc_datetime
      ], []}}
  ])
  ```

  ## Configuration

  To use this module, you'll need to configure your application to switch between Snowflake and local database:

  ```elixir
  # config/config.exs
  config :my_app, MyApp.Repo

  # config/test.exs
  config :my_app, MyApp.Repo,
    use_local_db?: true,
    adapter: Ecto.Adapters.Postgres
  ```

  ## Arguments

  * `repo` - The Ecto.Repo module to use for migrations
  * `modules` - A list of either:
    - Ecto.Schema modules
    - Tuples of `{module, {source, fields, primary_key}}` where:
      - `module` is the module name
      - `source` is the table name
      - `fields` is a keyword list of field names and types
      - `primary_key` is a list of primary key field names

  ## Returns

  * `:ok` when migrations are successfully generated and run
  """
  defmacro generate_migrations(repo, modules) do
    quote bind_quoted: [repo: repo, modules: modules] do
      # Import needed modules to avoid nesting
      alias Ecto.Migration
      alias Ecto.Migrator
      alias Ecto.Type

      modules
      |> Enum.with_index()
      |> Enum.each(fn
        {{module, {source, fields, primary_key}}, index} ->
          defmodule Module.concat(module, Migration) do
            use Migration

            @primary_key primary_key
            @source source
            @fields fields

            def change do
              primary_keys = [@primary_key]

              create table(@source, primary_key: false) do
                for {name, type} <- @fields do
                  field_type = Type.type(type)
                  field_source = name
                  add(field_source, type, primary_key: name in primary_keys)
                end
              end
            end
          end

          Migrator.up(
            repo,
            index,
            Module.concat(module, Migration),
            log: :info,
            skip_table_creation: false
          )

        {module, index} ->
          defmodule Module.concat(module, Migration) do
            use Migration

            @module module

            def change do
              primary_keys = @module.__schema__(:primary_key)

              create table(@module.__schema__(:source), primary_key: false) do
                for field <- @module.__schema__(:fields) do
                  type =
                    :type
                    |> @module.__schema__(field)
                    |> Type.type()

                  field_source = @module.__schema__(:field_source, field)
                  add(field_source, type, primary_key: field in primary_keys)
                end
              end
            end
          end

          Migrator.up(
            repo,
            index,
            Module.concat(module, Migration),
            log: :info,
            skip_table_creation: false
          )
      end)
    end
  end
end
