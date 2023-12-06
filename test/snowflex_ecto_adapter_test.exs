defmodule SnowflexEctoAdapterTest do
  use ExUnit.Case

  test "handles casting maybe date type to date" do
    assert [date_decode, {:maybe, :date}] =
             Snowflex.EctoAdapter.loaders({:maybe, :date}, {:maybe, :date})

    assert is_function(date_decode, 1)
    assert {:ok, ~D[2023-10-23]} = date_decode.("2023-10-23")
  end

  test "handles nil values" do
    [:integer, :utc_datetime, :naive_datetime, :decimal, :float, :date, :id, :time]
    |> Enum.each(fn type ->
      [decode_fn | _] = Snowflex.EctoAdapter.loaders(type, nil)
      assert {:ok, nil} == decode_fn.(nil)
    end)
  end
end
