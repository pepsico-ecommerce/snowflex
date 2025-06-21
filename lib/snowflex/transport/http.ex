defmodule Snowflex.Transport.Http do
  @moduledoc """
  REST API transport implementation for Snowflake.
  See: https://docs.snowflake.com/en/developer-guide/sql-api/reference

  ## Configuration Options

  The HTTP transport supports the following configuration options:

  ### Required Options
  * `:account_name` - Your Snowflake account identifier (e.g., "my-org-my-account")
  * `:username` - Your Snowflake username
  * `:private_key_path` - Path to your private key file (PEM format)
  * `:public_key_fingerprint` - Fingerprint of your public key

  ### Optional Options
  * `:database` - Default database to use
  * `:schema` - Default schema to use
  * `:warehouse` - Default warehouse to use
  * `:role` - Default role to use
  * `:timeout` - Query timeout in milliseconds (default: 45 seconds)
  * `:token_lifetime` - JWT token lifetime in milliseconds (default: 10 minutes)
  * `:private_key_password` - Password for the private key (if encrypted)

  ## Account Name Handling

  The transport automatically handles different Snowflake account name formats for JWT token generation:

  * For global accounts (e.g., "account-123.global.snowflakecomputing.com"):
    - Extracts the account identifier before the first hyphen
    - Example: "account-123" becomes "ACCOUNT"

  * For regional accounts (e.g., "account.us-east-1.snowflakecomputing.com"):
    - Extracts the account identifier before the first dot
    - Example: "account.us-east-1" becomes "ACCOUNT"


  ## Authentication

  The transport uses JWT authentication with RSA key pairs. The private key must be in PEM format
  and the public key fingerprint must be registered with Snowflake.

  ## Example Configuration

  ```elixir
  config :my_app, MyApp.Repo,
    adapter: Snowflex,
    transport: Snowflex.Transport.Http,
    account_name: "my-org-my-account",
    username: "my_user",
    private_key_path: "/path/to/key.pem",
    public_key_fingerprint: "abc123...",
    database: "MY_DB",
    schema: "MY_SCHEMA",
    warehouse: "MY_WH",
    role: "MY_ROLE",
    timeout: :timer.seconds(30),
    token_lifetime: :timer.minutes(15)
  ```
  """
  @behaviour Snowflex.Transport
  use GenServer

  alias JOSE.JWK
  alias JOSE.JWS
  alias JOSE.JWT
  alias Snowflex.Error
  alias Snowflex.Result

  require Logger

  @default_token_lifetime :timer.minutes(10)
  @default_timeout :timer.seconds(45)
  defmodule State do
    @moduledoc false
    @derive {Inspect, except: [:private_key, :token, :private_key_password]}

    defstruct [
      :account_name,
      :username,
      :private_key,
      :private_key_password,
      :req_client,
      :timeout,
      :token_lifetime,
      :current_statement,
      :current_partition,
      :token,
      :token_expires_at,
      :database,
      :schema,
      :warehouse,
      :role,
      :public_key_fingerprint,
      :result_metadata
    ]

    @type t :: %__MODULE__{
            account_name: String.t(),
            username: String.t(),
            private_key: String.t(),
            private_key_password: String.t() | nil,
            req_client: Req.Request.t(),
            timeout: integer(),
            token_lifetime: integer(),
            current_statement: String.t() | nil,
            current_partition: integer() | nil,
            token: String.t() | nil,
            token_expires_at: integer() | nil,
            database: String.t() | nil,
            schema: String.t() | nil,
            warehouse: String.t() | nil,
            role: String.t() | nil,
            public_key_fingerprint: String.t() | nil,
            result_metadata: map() | nil
          }
  end

  @impl Snowflex.Transport
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl Snowflex.Transport
  def execute_statement(pid, statement, params, opts) do
    opts = add_default_timeout(opts)
    GenServer.call(pid, {:execute, statement, params, opts}, opts[:timeout])
  end

  @impl Snowflex.Transport
  def declare(pid, statement, params, opts) do
    opts = add_default_timeout(opts)
    GenServer.call(pid, {:declare, statement, params, opts}, opts[:timeout])
  end

  @impl Snowflex.Transport
  def fetch(pid, cursor, opts) do
    opts = add_default_timeout(opts)
    GenServer.call(pid, {:fetch, cursor, opts}, opts[:timeout])
  end

  @impl Snowflex.Transport
  def disconnect(pid) do
    if Process.alive?(pid) do
      Process.exit(pid, :normal)
    end

    :ok
  end

  @doc """
  Execute an API Request via `Req.request/2`.

  This is useful when you need to execute an arbitrary API request against Snowflake's REST API.

  For example, if you want to use Snowfake Cortex, you might pass in:

  ```elixir
  Snowflex.Transport.Http.api(pid, %{
    method: :post,
    url: "/api/v2/cortex/analyst/message",
    json: %{messages: [%{role: "user", content: "Hello, how are you?"}]}
  })
  ```

  For more information on available `opts`, refer to `Req.new/1`.

  """
  @spec api(pid(), Req.request_opts()) :: {:ok, Req.response()} | {:error, Error.t()}
  def api(pid, opts) do
    GenServer.call(pid, {:api, opts})
  end

  defp add_default_timeout(opts) do
    Keyword.put_new(opts, :timeout, :timer.seconds(45))
  end

  # GenServer callbacks

  @impl GenServer
  def init(opts) do
    with {:ok, validated_opts} <- validate_opts(opts),
         {:ok, private_key} <- read_private_key(validated_opts),
         {:ok, state} <- init_state(validated_opts, private_key),
         {:ok, state} <- refresh_token(state) do
      check_connection(state)
    end
  end

  def handle_call({:api, opts}, _from, state) do
    with {:ok, state} <- ensure_valid_token(state),
         {:ok, response} <- execute_api_req(state, opts) do
      {:reply, {:ok, response}, state}
    else
      {:error, error} -> {:reply, {:error, error}, state}
    end
  end

  @impl GenServer
  def handle_call({:execute, statement, params, opts}, _from, state) do
    with {:ok, state} <- ensure_valid_token(state),
         # Execute the first request.
         # Once we have the initial body, we might need to make additional requests
         # to gather the partitions
         {:ok, body} <- fetch_statement(state, statement, params, opts),
         # We will reduce over the partitions to get the full result set
         {:ok, raw_result} <- gather_results(state, body, opts) do
      # And then format it for return
      result = format_response_body(raw_result)
      {:reply, {:ok, result}, state}
    else
      {:error, error} -> {:reply, {:error, error}, state}
    end
  end

  def handle_call({:declare, statement, params, opts}, _from, state) do
    with {:ok, state} <- ensure_valid_token(state),
         {:ok,
          %{
            "statementHandle" => statement_handle,
            "resultSetMetaData" => %{"partitionInfo" => partitions} = metadata
          }} <-
           fetch_statement(state, statement, params, opts) do
      {:reply, {:ok, length(partitions) - 1},
       %{
         state
         | current_statement: statement_handle,
           current_partition: 0,
           result_metadata: metadata
       }}
    else
      {:error, error} -> {:reply, {:error, error}, state}
    end
  end

  def handle_call(
        {:fetch, max_partition, opts},
        _from,
        %{
          current_partition: current_partition,
          current_statement: current_statement,
          result_metadata: metadata
        } = state
      )
      when current_partition <= max_partition do
    with {:ok, state} <- ensure_valid_token(state),
         {:ok, result} <-
           fetch_partition(state, current_statement, current_partition, opts) do
      result = format_response_body(result)

      result =
        Map.put(result, :columns, Enum.map(metadata["rowType"], & &1["name"]))

      {:reply, {:ok, result},
       %{
         state
         | current_partition: current_partition + 1
       }}
    else
      {:error, error} -> {:reply, {:error, error}, state}
    end
  end

  # No more partitions to call, but we do have a current statement
  def handle_call(
        {:fetch, _max_partition, _num_rows},
        _from,
        %{
          current_statement: current_statement
        } = state
      )
      when is_binary(current_statement) and byte_size(current_statement) > 0 do
    {:reply, {:halt, %Result{}}, state}
  end

  def handle_call({:fetch, _max_partition, _num_rows}, _from, state) do
    {:reply, {:error, %Error{message: "No active statement"}}, state}
  end

  ## Query helpers

  defp gather_results(
         state,
         %{
           "resultSetMetaData" => %{"partitionInfo" => partition_info},
           "statementHandle" => statement_handle
         } = body,
         opts
       )
       when is_list(partition_info) and length(partition_info) > 1 do
    initial_data = Map.get(body, "data", [])

    partition_count = length(partition_info)
    rest_partitions = Enum.to_list(1..(partition_count - 1)//1)
    max_concurrency = System.schedulers_online()

    # Fetch partitions in parallel
    Task.Supervisor.async_stream_nolink(
      Snowflex.TaskSupervisor,
      rest_partitions,
      fn partition_index ->
        fetch_partition(state, statement_handle, partition_index, opts)
      end,
      max_concurrency: max_concurrency,
      ordered: true,
      timeout: opts[:timeout],
      on_timeout: :kill_task
    )
    |> Enum.reduce_while({:ok, initial_data}, fn
      {:ok, {:ok, partition_body}}, {:ok, acc_data} ->
        # Extract data from this partition and add to accumulated data
        partition_data = Map.get(partition_body, "data", [])
        {:cont, {:ok, acc_data ++ partition_data}}

      {:ok, {:error, error}}, _acc ->
        {:halt, {:error, error}}

      {:exit, reason}, _acc ->
        {:halt, {:error, %Error{message: "Task failed: #{inspect(reason)}"}}}
    end)
    |> then(fn
      {:ok, merged_data} ->
        # Update the original body with the merged data
        merged_body = Map.put(body, "data", merged_data)
        {:ok, merged_body}

      {:error, error} ->
        {:error, error}
    end)
  end

  # There was only one partition, so we can return the body as is
  defp gather_results(_state, body, _opts) do
    {:ok, body}
  end

  # Init/Config Helpers
  defp validate_opts(opts) do
    Enum.reduce_while(
      [:account_name, :username, :private_key_path, :public_key_fingerprint],
      {:ok, opts},
      fn
        key, validated_opts ->
          case Keyword.fetch(opts, key) do
            {:ok, value} when is_binary(value) and byte_size(value) > 0 ->
              {:cont, validated_opts}

            _any ->
              {:halt, {:stop, %Error{message: "Missing required option: #{key}"}}}
          end
      end
    )
  end

  defp init_state(validated_opts, private_key) do
    {:ok,
     %State{
       account_name: Keyword.fetch!(validated_opts, :account_name),
       username: Keyword.fetch!(validated_opts, :username),
       public_key_fingerprint: Keyword.fetch!(validated_opts, :public_key_fingerprint),
       private_key: private_key,
       private_key_password: Keyword.get(validated_opts, :private_key_password, ~c""),
       current_statement: nil,
       timeout: Keyword.get(validated_opts, :timeout, @default_timeout),
       token_lifetime: Keyword.get(validated_opts, :token_lifetime, @default_token_lifetime),
       database: Keyword.get(validated_opts, :database),
       schema: Keyword.get(validated_opts, :schema),
       warehouse: Keyword.get(validated_opts, :warehouse),
       role: Keyword.get(validated_opts, :role)
     }}
  end

  defp check_connection(state) do
    case fetch_statement(state, "SELECT 1", %{}, timeout: state.timeout) do
      {:ok, _body} ->
        {:ok, state}

      {:error, error} ->
        {:stop, error}
    end
  end

  defp read_private_key(validated_opts) do
    path = Keyword.fetch!(validated_opts, :private_key_path)

    case File.read(path) do
      {:ok, key} ->
        {:ok, key}

      {:error, reason} ->
        {:stop, %Error{message: "Failed to read private key: #{inspect(reason)}"}}
    end
  end

  # Token helpers

  defp ensure_valid_token(state) do
    now = System.system_time(:second)

    if is_nil(state.token) or now >= state.token_expires_at do
      refresh_token(state)
    else
      {:ok, state}
    end
  end

  defp refresh_token(state) do
    now = System.system_time(:second)
    expires_at = now + state.token_lifetime

    account_id = prepare_account_name_for_jwt(state.account_name)
    username = String.upcase(state.username)

    [pem_entry] = :public_key.pem_decode(state.private_key)
    private_key = :public_key.pem_entry_decode(pem_entry, state.private_key_password)
    jwk = JWK.from_key(private_key)

    claims = %{
      "iss" => "#{account_id}.#{username}.SHA256:#{state.public_key_fingerprint}",
      "sub" => "#{account_id}.#{username}",
      "iat" => now,
      "exp" => expires_at
    }

    jws = %{"alg" => "RS256"}
    jwt = JWT.sign(jwk, jws, claims)
    {_, token} = JWS.compact(jwt)

    state = %{
      state
      | token: token,
        token_expires_at: expires_at,
        req_client: build_req_client(state.account_name, token)
    }

    {:ok, state}
  end

  defp prepare_account_name_for_jwt(raw_account) do
    account =
      if String.contains?(raw_account, ".global") do
        case String.split(raw_account, "-", parts: 2) do
          [account_id | _] -> account_id
          _ -> raw_account
        end
      else
        case String.split(raw_account, ".", parts: 2) do
          [account_id | _] -> account_id
          _ -> raw_account
        end
      end

    String.upcase(account)
  end

  # HTTP

  defp build_req_client(account_name, token) do
    base_url = "https://#{account_name}.snowflakecomputing.com"

    Req.new(
      base_url: base_url,
      headers: [
        {"Authorization", "Bearer #{token}"},
        {"Content-Type", "application/json"},
        {"Accept", "application/json"},
        {"User-Agent", "snowflex/1.0.0"},
        {"X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT"}
      ]
    )
  end

  defp format_response_body(body) do
    case body do
      %{
        "resultSetMetaData" => %{
          "rowType" => [%{"name" => "number of rows inserted"}]
        },
        "stats" => %{
          "numRowsInserted" => num_rows
        }
      } ->
        %Result{columns: [], rows: nil, num_rows: num_rows, metadata: %{}}

      %{
        "resultSetMetaData" => %{
          "rowType" => [%{"name" => "number of rows deleted"}]
        },
        "stats" => %{
          "numRowsDeleted" => num_rows
        }
      } ->
        %Result{columns: [], rows: nil, num_rows: num_rows, metadata: %{}}

      %{"data" => data, "resultSetMetaData" => metadata} ->
        columns = Enum.map(metadata["rowType"], & &1["name"])
        rows = data

        %Result{
          columns: columns,
          rows: rows,
          num_rows: length(rows),
          metadata: metadata
        }

      # Calls to additional partitions will not bring back the result set metadata
      %{"data" => data} ->
        %Result{rows: data, num_rows: length(data)}

      _any ->
        %Result{messages: [body["message"] || "Query executed successfully"]}
    end
  end

  # HTTP Calls

  defp execute_api_req(state, opts) do
    case Req.request(state.req_client, opts) do
      {:ok, response} ->
        {:ok, response}

      {:error, exception} ->
        {:error,
         %Error{
           message: inspect(exception),
           code: "HTTP_ERROR",
           metadata: %{opts: opts}
         }}
    end
  end

  defp fetch_statement(state, statement, params, opts) do
    req_body = %{
      statement: statement,
      timeout: opts[:timeout],
      database: state.database,
      schema: state.schema,
      warehouse: state.warehouse,
      role: state.role,
      bindings: params_to_bindings(params),
      parameters: request_params(opts)
    }

    url = "/api/v2/statements"

    case Req.post(state.req_client, url: url, json: req_body, receive_timeout: opts[:timeout]) do
      {:ok, %{status: status, body: body}} when status in 200..299 ->
        {:ok, body}

      {:ok, %{body: %{"code" => code, "message" => message}} = body} ->
        {:error,
         %Error{
           message: String.replace(message, ~r/\n/, " "),
           code: code,
           sql_state: Map.get(body, "sqlState"),
           metadata: %{
             request: req_body,
             response: body,
             opts: opts
           }
         }}

      {:ok, %{status: status, body: body}} ->
        {:error,
         %Error{
           code: status,
           message: inspect(body),
           metadata: %{
             request: req_body,
             response: body,
             opts: opts
           }
         }}

      {:error, exception} ->
        {:error,
         %Error{
           message: inspect(exception),
           code: "HTTP_ERROR",
           metadata: %{request: req_body, opts: opts}
         }}
    end
  end

  defp fetch_partition(state, statement_handle, partition_index, opts) do
    url = "/api/v2/statements/#{statement_handle}"
    params = %{partition: partition_index}

    case Req.get(state.req_client, url: url, params: params, receive_timeout: opts[:timeout]) do
      {:ok, %{status: status, body: partition_body}} when status in 200..299 ->
        {:ok, partition_body}

      {:ok, %{status: status, body: error_body}} ->
        {:error, %Error{message: "HTTP #{status}: #{inspect(error_body)}"}}

      {:error, exception} ->
        Logger.warning("Failed to fetch partition #{partition_index}: #{inspect(exception)}")
        {:error, %Error{message: "Failed to fetch partition: #{inspect(exception)}"}}
    end
  end

  defp params_to_bindings(params) do
    params
    |> Enum.with_index(1)
    |> Map.new(fn {value, index} ->
      {"#{index}", value}
    end)
  end

  @default_request_params %{
    "TIME_OUTPUT_FORMAT" => "HH24:MI:SS.FF",
    "TIMESTAMP_OUTPUT_FORMAT" => "YYYY-MM-DDTHH24:MI:SS.FFTZH:TZM",
    "TIMESTAMP_NTZ_OUTPUT_FORMAT" => "YYYY-MM-DDTHH24:MI:SS.FF",
    "DATE_OUTPUT_FORMAT" => "YYYY-MM-DD",
    "MULTI_STATEMENT_COUNT" => "0"
  }

  defp request_params(opts) do
    case Keyword.get(opts, :query_tag) do
      tag when is_binary(tag) and byte_size(tag) > 0 ->
        Map.put(@default_request_params, "QUERY_TAG", tag)

      _any ->
        @default_request_params
    end
  end
end
