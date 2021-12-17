defmodule Snowflex.Params do
  @moduledoc """
  Provides shared functions for parameter parsing
  """

  @string_types ~w(
    sql_char
    sql_wchar
    sql_varchar
    sql_wvarchar
    sql_wlongvarchar
  )a

  def prepare(params) do
    Enum.map(params, &prepare_param/1)
  end

  def prepare_param({type, values}) when not is_list(values) do
    prepare_param({type, [values]})
  end

  def prepare_param({{type_atom, _size} = type, values}) when type_atom in @string_types do
    {type, Enum.map(values, &null_or_charlist/1)}
  end

  def prepare_param({type, values}) do
    {type, Enum.map(values, &null_or_any/1)}
  end

  ## Helpers

  defp null_or_charlist(nil) do
    :null
  end

  defp null_or_charlist(val) do
    to_charlist(val)
  end

  defp null_or_any(nil) do
    :null
  end

  defp null_or_any(any) do
    any
  end
end
