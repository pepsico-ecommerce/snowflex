defmodule Snowflex.MigrationGenerator do
  @moduledoc """
  This module provides macros to autmoatically generate and start migrations based off Ecto Schema.

  This is helpful for building tests of Snowflake schema using another database as the backend.
  """
  defmacro generate_migrations(repo, modules) do
    quote bind_quoted: [repo: repo, modules: modules] do
      for {module, index} <- Enum.with_index(modules) do
        defmodule Module.concat(module, Migration) do
          use Ecto.Migration

          @module module

          def change do
            primary_keys = @module.__schema__(:primary_key)

            create table(@module.__schema__(:source), primary_key: false) do
              for field <- @module.__schema__(:fields) do
                type =
                  :type
                  |> @module.__schema__(field)
                  |> ecto_type_to_db_type()

                field_source = @module.__schema__(:field_source, field)
                add(field_source, type, primary_key: field in primary_keys)
              end
            end
          end

          defp ecto_type_to_db_type({:parameterized, Ecto.Enum, _}), do: :string
          defp ecto_type_to_db_type(any), do: any
        end

        Ecto.Migrator.up(
          repo,
          index,
          Module.concat(module, Migration),
          log: :info,
          skip_table_creation: false
        )
      end
    end
  end
end
