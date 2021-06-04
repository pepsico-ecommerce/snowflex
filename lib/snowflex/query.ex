defmodule Snowflex.Query do
  @moduledoc """
  The module creates a structured data type for queries for easier execution.
  """

  use Ecto.Schema
  import Ecto.Changeset

  alias Snowflex.Types.SQLParam

  @type query_attrs :: %{
          query_string: String.t(),
          params: nil | list()
        }

  @required_fields ~w(query_string)a
  @fields @required_fields ++ ~w(params)a

  @primary_key false
  embedded_schema do
    field(:query_string, :string)
    field(:params, {:array, SQLParam})
  end

  @doc """
  Build a new Snowflex.Query struct
  """
  @spec create(attrs :: query_attrs()) :: {:ok, %__MODULE__{}} | {:error, Ecto.Changeset.t()}
  def create(attrs) do
    %__MODULE__{}
    |> cast(attrs, @fields)
    |> validate_required(@required_fields)
    |> case do
      changeset = %Ecto.Changeset{valid?: true} ->
        query = apply_changes(changeset)
        params = dump_params(query.params)

        {:ok, %__MODULE__{query | params: params}}

      changeset ->
        {:error, changeset}
    end
  end

  # HELPERS

  defp dump_params(nil), do: nil
  defp dump_params(params), do: Enum.flat_map(params, &Map.to_list/1)
end
