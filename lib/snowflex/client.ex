defmodule Snowflex.Client do
  @callback start_link(Keyword.t()) :: {:ok, pid()}

  @callback sql_query(pid(), iodata(), Keyword.t()) ::
              {:ok, {:selected, [binary()], [tuple()]}}
              | {:ok, {:selected, [binary()], [tuple()], [{binary()}]}}
              | {:ok, {:updated, non_neg_integer()}}
              | {:error, Error.t()}
  @callback param_query(pid(), iodata(), Keyword.t(), Keyword.t()) ::
              {:ok, {:selected, [binary()], [tuple()]}}
              | {:ok, {:selected, [binary()], [tuple()], [{binary()}]}}
              | {:ok, {:updated, non_neg_integer()}}
              | {:error, Error.t()}

  @callback disconnect(pid()) :: :ok

  defmacro __using__(_opts) do
    quote do
      @behaviour Snowflex.Client
    end
  end
end
