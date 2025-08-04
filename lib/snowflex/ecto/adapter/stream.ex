defmodule Snowflex.Ecto.Adapter.Stream do
  @moduledoc false

  @type t :: %__MODULE__{
          meta: map(),
          statement: String.t(),
          params: list(),
          opts: Keyword.t()
        }

  defstruct [:meta, :statement, :params, :opts]

  @doc false
  @spec build(meta :: map(), statement :: String.t(), params :: list(), opts :: Keyword.t()) ::
          t()
  def build(meta, statement, params, opts) do
    %__MODULE__{meta: meta, statement: statement, params: params, opts: opts}
  end
end

alias Snowflex.Ecto.Adapter.Stream

defimpl Enumerable, for: Stream do
  @spec count(Stream.t()) :: {:error, module()}
  def count(_stream), do: {:error, __MODULE__}

  @spec member?(Stream.t(), term()) :: {:error, module()}
  def member?(_stream, _value), do: {:error, __MODULE__}

  @spec slice(Stream.t()) :: {:error, module()}
  def slice(_stream), do: {:error, __MODULE__}

  @spec reduce(Stream.t(), Enumerable.acc(), Enumerable.reducer()) :: Enumerable.result()
  def reduce(stream, acc, fun) do
    %Stream{meta: meta, statement: statement, params: params, opts: opts} = stream
    Snowflex.reduce(meta, statement, params, opts, acc, fun)
  end
end

defimpl Collectable, for: Stream do
  @type collector_fun :: (term(), :done | {:cont, term()} -> {term(), term()})

  @spec into(Stream.t()) :: {list(), collector_fun()}
  def into(stream) do
    %Stream{meta: meta, statement: statement, params: params, opts: opts} = stream
    {state, fun} = Snowflex.into(meta, statement, params, opts)
    {state, make_into(fun, stream)}
  end

  @spec make_into((list(), :done | {:cont, term()} -> {list(), list()}), Stream.t()) ::
          (list(), :done | {:cont, term()} -> {list(), Stream.t()})
  defp make_into(fun, stream) do
    fn
      state, :done ->
        _result = fun.(state, :done)
        stream

      state, acc ->
        fun.(state, acc)
    end
  end
end
