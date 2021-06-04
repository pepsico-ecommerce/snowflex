defmodule Snowflex.Types.SQLParam do
  @moduledoc """
  Ecto.Type to cast params into odbc acceptable params
  """
  use Ecto.Type

  @impl true
  def type, do: :map

  @impl true
  def cast(term) when is_binary(term), do: {:ok, %{{:sql_varchar, 250} => term}}
  def cast(term) when is_integer(term), do: {:ok, %{sql_integer: term}}
  def cast(term = %Date{}), do: {:ok, %{{:sql_varchar, 250} => Date.to_iso8601(term)}}
  def cast(_term), do: :error

  @impl true
  def load(term), do: {:ok, term}

  @impl true
  def dump(term), do: {:ok, term}
end
