if Code.ensure_loaded?(Req) do
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

    @behaviour Snowflex.Transport

    @impl true
    def connect(opts) do
      connection = Keyword.fetch!(opts, :connection)

      query_opts = Keyword.take(connection, [:warehouse, :role])
      account = Keyword.fetch!(connection, :account)
      key = Keyword.fetch!(connection, :key)

      client =
        Req.new(
          base_url: base_url(account),
          auth: {:bearer, key},
          headers: @headers
        )

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
      |> Req.post!(url: "statements", json: build_query(query, opts))
      |> handle_response(query)
    end

    @impl true
    def param_query(conn, query, params) do
      %{client: client, query_opts: opts} = Agent.get(conn, & &1)

      client
      |> Req.post!(url: "statements", json: build_query(query, params, opts))
      |> handle_response(query)
    end

    # Param Helpers

    @doc "Construct an integer parameter."
    @spec int_param(integer()) :: param()
    def int_param(val), do: {"FIXED", to_string(val)}

    @doc "Construct a string parameter."
    @spec string_param(String.t(), non_neg_integer()) :: param()
    def string_param(val, _length \\ 250), do: {"TEXT", val}

    defp base_url(account_id) do
      "https://#{account_id}.snowflakecomputing.com/api"
    end

    defp handle_response(%{status: status, body: body} = resp, query) when status in 200..299 do
      case body do
        %{"data" => %{"rowset" => rows, "rowtype" => columns}} ->
          headers = Enum.map(columns, & &1["name"])
          row_tuples = Enum.map(rows, &List.to_tuple/1)

          {:ok, Result.from_headers_and_rows(query, headers, row_tuples)}

        %{"data" => %{"rowset" => []}} ->
          {:ok, Result.from_headers_and_rows(query, [], [])}

        %{"data" => %{"statementHandle" => _handle, "complete" => true}} ->
          # For DDL/DML statements that don't return rows
          {:ok, Result.from_update(query, 0)}

        _ ->
          {:error, "Unexpected response format: #{inspect(resp)}"}
      end
    end

    defp handle_response(%{status: status, body: body}, _query) do
      error_msg =
        case body do
          %{"message" => msg} -> msg
          _ -> "HTTP #{status}: #{inspect(body)}"
        end

      {:error, error_msg}
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

    defp put_unless_nil(map, _key, nil), do: map
    defp put_unless_nil(map, key, value), do: Map.put(map, key, value)
  end
end
