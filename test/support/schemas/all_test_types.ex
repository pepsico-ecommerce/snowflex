defmodule Snowflex.AllTestTypes do
  @moduledoc """
  Test schema with all supported Snowflake data types for comprehensive testing.
  """
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :integer, read_after_writes: true}
  schema "test_all_types" do
    field(:col_number, :decimal)
    field(:col_int, :integer)
    field(:col_float, :float)
    field(:col_boolean, :boolean)
    field(:col_varchar, :string)

    field(:col_binary, :binary)

    field(:col_date, :date)
    field(:col_time, :time)

    field(:col_timestamp_ltz, :utc_datetime)
    field(:col_timestamp_ntz, :naive_datetime)
    field(:col_timestamp_tz, :utc_datetime)

    field(:col_variant, :map)
    field(:col_object, :map)
    field(:col_array, :map)

    field(:col_geography, :string)
  end

  @spec changeset(Ecto.Schema.t(), map()) :: Ecto.Changeset.t()
  def changeset(schema, attrs) do
    schema
    |> cast(attrs, [
      :col_number,
      :col_int,
      :col_float,
      :col_boolean,
      :col_varchar,
      :col_binary,
      :col_date,
      :col_time,
      :col_timestamp_ltz,
      :col_timestamp_ntz,
      :col_timestamp_tz,
      :col_variant,
      :col_object,
      :col_array,
      :col_geography
    ])
  end
end
