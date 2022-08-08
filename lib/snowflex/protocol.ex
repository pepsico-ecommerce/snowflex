defmodule Snowflex.Protocol do
  @moduledoc """
  Provides shared functions for parameter parsing
  """

  def annotate_params(params) do
    Enum.map(params, &annotate_param/1)
  end

  defp annotate_param(param) when is_integer(param), do: {:sql_integer, [null_or_any(param)]}

  defp annotate_param(param) when is_binary(param),
    do: {{:sql_varchar, String.length(param)}, [null_or_charlist(param)]}

  defp null_or_charlist(nil), do: :null
  defp null_or_charlist(value), do: to_charlist(value)

  defp null_or_any(nil), do: :null
  defp null_or_any(value), do: value
end
