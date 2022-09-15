defmodule Snowflex.Client.Mock.Responser do
  @callback handle_response(iodata(), map()) ::
              {:ok, {:selected, [binary()], [tuple()]}}
              | {:ok, {:selected, [binary()], [tuple()], [{binary()}]}}
              | {:ok, {:updated, non_neg_integer()}}
              | {:error, Error.t()}

  defmacro __using__(_opts) do
    quote do
      @behaviour Snowflex.Client.Mock.Responser

      def handle_response("SELECT /* snowflex:heartbeat */ 1;", _),
        do: {:ok, {:selected, [1], [[1]]}}

      def handle_response(statement, _) do
        throw("You need to provide response for statement #{statement}")
      end

      defoverridable handle_response: 2
    end
  end
end
