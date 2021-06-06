defmodule Snowflex.Query do
  @moduledoc """
  The module creates a structured data type for queries for easier execution.
  """

  @type t :: %__MODULE__{
          query_string: String.t(),
          params: nil | list(Snowflex.query_param())
        }

  @type query_attrs :: %{
          query_string: String.t(),
          params: nil | list(String.t() | integer() | Date.t())
        }

  defstruct query_string: nil, params: nil

  @doc """
  Build a new Snowflex.Query struct. Will raise if unable to create.
  """
  @spec create!(attrs :: query_attrs()) :: t()
  def create!(attrs) do
    attrs
    |> cast_params()
    |> build_struct()
  end

  @doc """
  Build a new Snowflex.Query struct
  """
  @spec create(attrs :: query_attrs()) :: {:ok, t()} | {:error, :invalid_query}
  def create(attrs) do
    try do
      query = create!(attrs)

      {:ok, query}
    rescue
      e in ArgumentError ->
        {:error, e.message}
    end
  end

  # HELPERS

  defp cast_params(attrs = %{params: params}) when not is_nil(params) do
    Map.put(attrs, :params, Enum.map(params, &do_param_cast/1))
  end

  defp cast_params(attrs), do: attrs

  defp do_param_cast(param) when is_binary(param), do: {{:sql_varchar, 250}, param}
  defp do_param_cast(param) when is_integer(param), do: {:sql_integer, param}
  defp do_param_cast(param = %Date{}), do: {{:sql_varchar, 250}, Date.to_iso8601(param)}
  defp do_param_cast(_param), do: raise(ArgumentError, "unsupported parameter type given")

  defp build_struct(attrs) do
    unless Map.has_key?(attrs, :query_string) do
      raise(ArgumentError, "must provide :query_string to build Query")
    end

    struct(__MODULE__, attrs)
  end
end
