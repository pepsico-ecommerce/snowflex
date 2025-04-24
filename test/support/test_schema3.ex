defmodule TestSchema3 do
  @moduledoc """
  Test schema representing a permalink with a binary field.
  """
  use Ecto.Schema

  schema "schema3" do
    field(:binary, :binary)
  end
end
