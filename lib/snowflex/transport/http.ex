if Code.ensure_loaded?(Tesla) do
  defmodule Snowflex.Transport.HTTP do
    @moduledoc false

    alias Snowflex.Result

    @typedoc """
    The following Snowflake parameter types are supported:

    - `FIXED`
    - `TEXT`

    The following types are currently **unsupported**:

    - `REAL`
    - `BINARY`
    - `BOOLEAN`
    - `DATE`
    - `TIME`
    - `TIMESTAMP_TZ`
    - `TIMESTAMP_LTZ`
    - `TIMESTAMP_NTZ`

    Query parameters are always serialized as strings. You can read more
    about Snowflake parameter binding [here][params].

    [params]: https://docs.snowflake.com/en/developer-guide/sql-api/guide.html#label-sql-api-bind-variables
    """
    @type param :: {type :: String.t(), value :: String.t()}

    defmodule Conn do
      @moduledoc false

      defstruct [:client, :query_opts]
    end

    config = Mix.Project.config()
    @app_version Keyword.fetch!(config, :version)

    @headers [
      {"Accept", "application/json"},
      {"User-Agent", "snowflex/#{@app_version}"},
      {"X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT"}
    ]

    @middleware [
      {Tesla.Middleware.Headers, @headers},
      Tesla.Middleware.JSON
    ]

    @behaviour Snowflex.Transport

    @impl true
    def connect(opts) do
      connection = Keyword.fetch!(opts, :connection)

      query_opts = Keyword.take(connection, [:warehouse, :role])
      account = Keyword.fetch!(connection, :account)
      key = Keyword.fetch!(connection, :key)

      client =
        [
          {Tesla.Middleware.BaseUrl, base_url(account)},
          {Tesla.Middleware.BearerAuth, token: key}
        ]
        |> Enum.concat(@middleware)
        |> Tesla.client()

      conn = %Conn{client: client, query_opts: query_opts}
      {:ok, pid} = Agent.start_link(fn -> conn end)

      pid
    end

    @impl true
    def disconnect(conn), do: Agent.stop(conn)

    @impl true
    def sql_query(conn, query) do
      %{client: client, query_opts: opts} = Agent.get(conn, & &1)

      client
      |> Tesla.post("statements", build_query(query, opts))
      |> handle_response(query)
    end

    @impl true
    def param_query(conn, query, params) do
      %{client: client, query_opts: opts} = Agent.get(conn, & &1)

      client
      |> Tesla.post("statements", build_query(query, params, opts))
      |> handle_response(query)
    end

    # Param Helpers

    @doc "Construct an integer parameter."
    @spec int_param(integer()) :: param()
    def int_param(val), do: {"FIXED", to_string(val)}

    @doc "Construct a string parameter."
    @spec string_param(String.t(), non_neg_integer()) :: param()
    def string_param(val, _length \\ 250), do: {"TEXT", val}

    # ---

    defp base_url(account_id) do
      "https://#{account_id}.snowflakecomputing.com/api"
    end

    defp handle_response({:error, reason}, _), do: {:error, reason}

    defp handle_response(resp, query) do
      IO.inspect(%{resp: resp, query: query})

      # FIXME: handle other responses
      raise "not implemented"
    end

    defp build_query(query, params \\ [], opts) do
      %{
        "statement" => query,
        "resultSetMetaData" => %{"format" => "jsonv2"},
        "bindings" => Map.new(params, &param_to_binding/1)
      }
      |> put_unless_nil("warehouse", opts[:warehouse])
      |> put_unless_nil("role", opts[:role])
    end

    defp param_to_binding({type, value}) do
      %{
        "type" => type,
        "value" => value
      }
    end

    defp put_unless_nil(map, _, nil), do: map
    defp put_unless_nil(map, key, value), do: Map.put(map, key, value)
  end
end
