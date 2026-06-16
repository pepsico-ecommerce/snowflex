defmodule Snowflex.VariantField do
  @moduledoc """
  Shared predicate for identifying Ecto field types that map to Snowflake's
  VARIANT semi-structured type (`:map`, `{:map, _}`, `{:array, _}`).

  This is the single source of truth used by both `Snowflex` (for insert/update
  via schema meta) and `Snowflex.Ecto.Adapter.Connection` (for update_all SQL
  generation).
  """

  @doc """
  Returns `true` if the given Ecto field type maps to a Snowflake VARIANT column.

  ## Examples

      iex> Snowflex.VariantField.variant_field?(:map)
      true

      iex> Snowflex.VariantField.variant_field?({:map, :string})
      true

      iex> Snowflex.VariantField.variant_field?({:array, :integer})
      true

      iex> Snowflex.VariantField.variant_field?(:string)
      false

  """
  @spec variant_field?(term()) :: boolean()
  def variant_field?(:map), do: true
  def variant_field?({:map, _}), do: true
  def variant_field?({:array, _}), do: true
  def variant_field?(_), do: false
end
