defmodule Snowflex.Query do
  @moduledoc false
  import Ecto.Changeset

  defmacro __using__(_opts) do
    quote do
      import Snowflex.Query
    end
  end

  defmacro snowflake_query(query) do
    quote do
      def sql_query, do: unquote(query)

      def execute_query do
        sql_query(sql_query())
      end

      def execute_query(binding_params) do
        param_query(sql_query(), binding_params)
      end
    end
  end

  def cast_results(data, schema) do
    Enum.map(data, &cast_row(&1, schema))
  end

  def int_param(val), do: {:sql_integer, val}
  def string_param(val, length \\ 250), do: {{:sql_varchar, length}, val}

  def sql_query(pool_name, query, post_process \\ fn x -> x end) do
    case Snowflex.sql_query(pool_name, query) do
      {:error, err} -> {:error, err}
      results -> results |> post_process.()
    end
  end

  def param_query(pool_name, query, params, post_process \\ fn x -> x end) do
    case Snowflex.param_query(pool_name, query, params) do
      {:error, err} -> {:error, err}
      results -> results |> post_process.()
    end
  end

  defp cast_row(row, schema) do
    schema
    |> struct()
    |> cast(row, schema.__schema__(:fields))
    |> apply_changes()
  end
end
