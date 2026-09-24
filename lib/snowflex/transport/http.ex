defmodule Snowflex.Transport.Http do
  @moduledoc """
  REST API transport implementation for Snowflake.
  See: https://docs.snowflake.com/en/developer-guide/sql-api/reference

  ## Configuration Options

  The HTTP transport supports the following configuration options:

  ### Required Options
  * `:account_name` - Your Snowflake account identifier (e.g., "my-org-my-account")
  * `:username` - Your Snowflake username
  * `:private_key_path` - Path to your private key file (PEM format) OR
  * `:private_key_from_string` - Your private key as a string (PEM format)

  ### Optional Options
  * `:public_key_fingerprint` - Fingerprint of your registered public key. When
    omitted, it is computed automatically from your private key (matching the
    `RSA_PUBLIC_KEY_FP` Snowflake stores), so you normally do not need to provide it.
  * `:database` - Default database to use
  * `:schema` - Default schema to use
  * `:warehouse` - Default warehouse to use
  * `:role` - Default role to use
  * `:timeout` - Query timeout in milliseconds (default: 45 seconds)
  * `:token_lifetime` - JWT token lifetime in milliseconds (default: 10 minutes)
  * `:private_key_password` - Password for the private key (if encrypted)
  * `:async_poll_interval` - Interval in milliseconds to poll for async execution status (default: 1000)
  * `:max_retries` - Maximum retry attempts for rate limits (default: 3)
  * `:retry_base_delay` - Base delay for exponential backoff in milliseconds (default: 1000)
  * `:retry_max_delay` - Maximum delay between retries in milliseconds (default: 8000)
  * `:connect_options` - Connection options for Finch pool configuration. Ignored when a `:finch` instance is supplied via `:req_options`, because Req forbids setting both.
  * `:req_options` - Additional options to pass to `Req.new/1` (e.g., `:plug` for testing, or `:finch` to route requests at a dedicated Finch pool)

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
  and the public key must be registered with Snowflake.

  ## Private Key Configuration

  Snowflex supports two ways to provide your private key for authentication:

  ### 1. File Path (traditional method)
  ```elixir
  config :my_app, MyApp.Repo,
    # ... other options ...
    private_key_path: "/path/to/your/private_key.pem"
  ```

  ### 2. String (inline method)
  ```elixir
  config :my_app, MyApp.Repo,
    # ... other options ...
    private_key_from_string: System.get_env("SNOWFLAKE_PRIVATE_KEY") || \"\"\"
    -----BEGIN PRIVATE KEY-----
    MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC...
    -----END PRIVATE KEY-----
    \"\"\"
  ```

  **Important notes:**
  - You must provide either `private_key_path` OR `private_key_from_string`, not both
  - Both options accept PEM format private keys
  - The string method is useful when deploying to environments where file system access is restricted or when storing keys in environment variables/secrets management systems

  ## Example Configuration

  ```elixir
  config :my_app, MyApp.Repo,
    adapter: Snowflex,
    transport: Snowflex.Transport.Http,
    account_name: "my-org-my-account",
    username: "my_user",
    private_key_path: "/path/to/key.pem",
    # OR alternatively use private_key_from_string instead of private_key_path:
    # private_key_from_string: "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----",
    # public_key_fingerprint is optional — derived from the private key when omitted.
    database: "MY_DB",
    schema: "MY_SCHEMA",
    warehouse: "MY_WH",
    role: "MY_ROLE",
    timeout: :timer.seconds(30),
    token_lifetime: :timer.minutes(15),
    # Retry configuration
    max_retries: 3,
    retry_base_delay: :timer.seconds(1),
    retry_max_delay: :timer.seconds(8)
  ```
  """
  @behaviour Snowflex.Transport
  use GenServer

  alias JOSE.JWK
  alias JOSE.JWS
  alias JOSE.JWT
  alias Snowflex.Error
  alias Snowflex.Result
  alias Snowflex.Transport.Http.KeyFingerprint

  require Logger

  @default_token_lifetime :timer.minutes(10)
  @default_timeout :timer.seconds(45)
  @call_timeout_grace :timer.seconds(5)
  # Refresh the cached JWT this long before it expires, so a request never goes
  # out with a token that lapses in flight.
  @token_refresh_margin :timer.seconds(30)
  defmodule State do
    @moduledoc false
    @derive {Inspect, except: [:private_key, :private_key_password, :auth_token]}

    defstruct [
      :account_name,
      :username,
      :private_key,
      :private_key_password,
      :timeout,
      :token_lifetime,
      :current_statement,
      :current_partition,
      :database,
      :schema,
      :warehouse,
      :role,
      :public_key_fingerprint,
      :result_metadata,
      :async_poll_interval,
      :max_retries,
      :retry_base_delay,
      :retry_max_delay,
      :connect_options,
      :req_options,
      :auth_token,
      :auth_token_expires_at
    ]

    @type t :: %__MODULE__{
            account_name: String.t(),
            username: String.t(),
            private_key: String.t(),
            private_key_password: String.t() | nil,
            timeout: integer(),
            token_lifetime: integer(),
            current_statement: String.t() | nil,
            current_partition: integer() | nil,
            database: String.t() | nil,
            schema: String.t() | nil,
            warehouse: String.t() | nil,
            role: String.t() | nil,
            public_key_fingerprint: String.t() | nil,
            result_metadata: map() | nil,
            async_poll_interval: non_neg_integer(),
            max_retries: non_neg_integer(),
            retry_base_delay: non_neg_integer(),
            retry_max_delay: non_neg_integer(),
            connect_options: Keyword.t(),
            req_options: Keyword.t(),
            auth_token: String.t() | nil,
            auth_token_expires_at: integer() | nil
          }
  end

  @impl Snowflex.Transport
  # HTTP transport does not care about connection state or sessions, we do not need ping
  def ping(_pid), do: {:ok, %Result{}}

  @impl Snowflex.Transport
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl Snowflex.Transport
  def execute_statement(pid, statement, params, opts) do
    opts = add_default_timeout(opts)

    try do
      GenServer.call(pid, {:execute, statement, params, opts}, opts[:timeout])
    catch
      :exit, {:timeout, _} ->
        {:error, Error.exception("#{statement} timed out after #{inspect(opts[:timeout])}")}

      :exit, reason ->
        {:error, Error.exception("#{statement} failed due to #{inspect(reason)}")}
    end
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
  def submit_async(pid, statement, params, opts) do
    opts = add_default_timeout(opts)
    call(pid, {:submit_async, statement, params, opts}, statement, opts)
  end

  @impl Snowflex.Transport
  def statement_status(pid, handle, opts) do
    opts = add_default_timeout(opts)
    call(pid, {:statement_status, handle, opts}, handle, opts)
  end

  @impl Snowflex.Transport
  def fetch_result(pid, handle, opts) do
    opts = add_default_timeout(opts)
    call(pid, {:fetch_result, handle, opts}, handle, opts)
  end

  @impl Snowflex.Transport
  def cancel_statement(pid, handle, opts) do
    opts = add_default_timeout(opts)
    call(pid, {:cancel_statement, handle, opts}, handle, opts)
  end

  # The handler bounds its own HTTP request with opts[:timeout]; give the
  # GenServer.call a grace period on top so a request that is merely slow
  # surfaces as the handler's {:error, _} instead of racing the call deadline and
  # exiting. Without the catch an exit propagates out of
  # Snowflex.Connection.handle_execute/4 and DBConnection re-raises it, so the
  # caller never sees a Snowflex.Error.
  defp call(pid, message, subject, opts) do
    GenServer.call(pid, message, call_timeout(opts[:timeout]))
  catch
    :exit, {:timeout, _} ->
      {:error, Error.exception("#{subject} timed out after #{inspect(opts[:timeout])}")}

    :exit, reason ->
      {:error, Error.exception("#{subject} failed due to #{inspect(reason)}")}
  end

  defp call_timeout(:infinity), do: :infinity
  defp call_timeout(timeout) when is_integer(timeout), do: timeout + @call_timeout_grace

  @impl Snowflex.Transport
  def disconnect(pid) do
    if Process.alive?(pid) do
      Process.exit(pid, :normal)
    end

    :ok
  end

  @impl Snowflex.Transport
  def deallocate(pid) do
    GenServer.cast(pid, :deallocate)
    :ok
  end

  @doc """
  Returns the current `t:Req.Request` for the transport.

  Useful when you need to execute an arbitrary API request against Snowflake's REST API.
  """
  @spec client(GenServer.server()) :: Req.Request.t()
  def client(pid) do
    GenServer.call(pid, :client)
  end

  @doc """
  Returns the current set of options for Req.

  This can be useful when you need to execute an arbitrary API request against Snowflake's REST API,
  but need more control over how the Req client is built.

  To keep Snowflex's internal Req usage simpler, we do *not* support the full gamut of options that `Req` does,
  only the subset we rely upon.  Your use case might necessitate changing/modifying/adding other options however.

  To route Snowflake requests at a dedicated `Finch` pool, pass it via `:req_options`, e.g.
  `req_options: [finch: [name: MyFinch]]`. The bare pool name (`req_options: [finch: MyFinch]`)
  is also accepted and normalized to the keyword form (Req 0.7 deprecated the bare name).
  When a `:finch` pool is provided this way, `Http` omits `:connect_options` so the two do
  not conflict (`Req` does not allow specifying `:connect_options` and `:finch` at the same
  time); the Finch pool then owns the connection configuration.

  `options/1` remains available when you need to build the Req client yourself and modify it further:

  ## Example:
  ```elixir
    {:ok, options} =
      :my_app
      |> Application.get_config(MyRepo)
      |> Snowflex.Transport.Http.options()

    options
    |> Keyword.delete(:connect_options)
    |> Keyword.put(:finch, name: MyFinch)
    |> Req.new()
  ```
  """
  @spec options(keyword) :: {:ok, keyword} | {:error, term()}
  def options(config) when is_list(config) do
    with {:ok, validated_opts, private_key} <- validate_and_read_private_key(config),
         {:ok, opts_with_fingerprint} <- resolve_fingerprint(validated_opts, private_key),
         {:ok, state} <- init_state(opts_with_fingerprint, private_key) do
      {:ok, build_options(state)}
    else
      {:stop, error} -> {:error, error}
    end
  end

  defp add_default_timeout(opts) do
    Keyword.put_new(opts, :timeout, :timer.seconds(45))
  end

  # GenServer callbacks

  @impl GenServer
  def init(opts) do
    with {:ok, validated_opts, private_key} <- validate_and_read_private_key(opts),
         {:ok, opts_with_fingerprint} <- resolve_fingerprint(validated_opts, private_key),
         {:ok, state} <- init_state(opts_with_fingerprint, private_key) do
      check_connection(state)
    end
  end

  def handle_call(:client, _from, state) do
    state = refresh_token(state)
    {:reply, build_req_client(state), state}
  end

  @impl GenServer
  def handle_call({:execute, statement, params, opts}, _from, state) do
    state = refresh_token(state)

    with {:ok, status, body} <- fetch_statement(state, statement, params, opts),
         #  After 45 seconds, Snowflake will return a 202 status code and a body with a statementHandle
         #  We need to poll for the result set
         {:ok, body} <- await_async_execution(state, status, body),
         # Once we have the initial body, we might need to make additional requests
         # to gather the partitions
         # We will reduce over the partitions to get the full result set
         {:ok, raw_result} <- gather_results(state, body, opts) do
      # And then format it for return
      result = format_response_body(raw_result)
      {:reply, {:ok, result}, state, :hibernate}
    else
      {:error, error} -> {:reply, {:error, error}, state}
    end
  end

  def handle_call({:declare, statement, params, opts}, _from, state) do
    state = refresh_token(state)

    # Long-running statements answer 202 before the result set exists, so poll
    # to completion (like :execute does) before reading partition metadata.
    with {:ok, status, body} <- fetch_statement(state, statement, params, opts),
         {:ok,
          %{
            "statementHandle" => statement_handle,
            "resultSetMetaData" => %{"partitionInfo" => partitions} = metadata
          }} <- await_async_execution(state, status, body) do
      {:reply, {:ok, length(partitions) - 1},
       %{
         state
         | current_statement: statement_handle,
           current_partition: 0,
           result_metadata: metadata
       }}
    else
      {:error, error} ->
        {:reply, {:error, error}, state}

      {:ok, body} ->
        error =
          Error.exception(
            "statement did not return a partitioned result set " <>
              "(multi-statement requests cannot be streamed): #{inspect(body)}"
          )

        {:reply, {:error, error}, state}
    end
  end

  # Fire-and-forget submission. Snowflake answers `async=true` with 202 and a
  # statement handle as soon as it accepts the statement, so this deliberately
  # does NOT call await_async_execution/3 — polling here is what pins the
  # connection for the statement's full duration. It also leaves
  # :current_statement alone: that field belongs to the cursor path, and
  # overwriting it would corrupt an in-flight stream on the same connection.
  def handle_call({:submit_async, statement, params, opts}, _from, state) do
    state = refresh_token(state)

    reply =
      case fetch_statement(state, statement, params, opts, async?: true) do
        {:ok, _status, %{"statementHandle" => handle} = body} when is_binary(handle) ->
          {:ok, handle_result(body, handle)}

        {:ok, _status, body} ->
          {:error,
           Error.exception(
             "Snowflake accepted the statement but returned no usable statementHandle: " <>
               inspect(body)
           )}

        {:error, error} ->
          {:error, error}
      end

    {:reply, reply, state, :hibernate}
  end

  def handle_call({:statement_status, handle, opts}, _from, state) do
    state = refresh_token(state)
    {:reply, request_statement_status(state, handle, opts), state, :hibernate}
  end

  def handle_call({:fetch_result, handle, opts}, _from, state) do
    state = refresh_token(state)
    {:reply, request_result(state, handle, opts), state, :hibernate}
  end

  def handle_call({:cancel_statement, handle, opts}, _from, state) do
    state = refresh_token(state)
    {:reply, request_cancel(state, handle, opts), state, :hibernate}
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
    state = refresh_token(state)

    case fetch_partition(state, current_statement, current_partition, opts) do
      {:ok, result} ->
        # Partition responses carry only data, so attach the statement's
        # metadata: columns for consumers and rowType so DBConnection.Query
        # decoding produces the same typed values as execute.
        result =
          result
          |> format_response_body()
          |> Map.put(:columns, Enum.map(metadata["rowType"], & &1["name"]))
          |> Map.put(:metadata, metadata)

        # Halt with the final partition: the cursor is known to be exhausted
        # once the last partition index is served, so streams contain exactly
        # one result per partition (no trailing empty result) and skip a
        # needless final fetch.
        reply =
          if current_partition == max_partition do
            {:halt, result}
          else
            {:ok, result}
          end

        {:reply, reply, %{state | current_partition: current_partition + 1}}

      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  # Safety net: fetch called with no partitions left (e.g. an empty
  # partitionInfo) but with a statement still declared.
  def handle_call(
        {:fetch, _max_partition, _num_rows},
        _from,
        %{
          current_statement: current_statement
        } = state
      )
      when is_binary(current_statement) and byte_size(current_statement) > 0 do
    {:reply, {:halt, %Result{}}, state, :hibernate}
  end

  def handle_call({:fetch, _max_partition, _num_rows}, _from, state) do
    {:reply, {:error, %Error{message: "No active statement"}}, state}
  end

  @impl GenServer
  def handle_cast(:deallocate, state) do
    {:noreply, %{state | current_statement: nil, current_partition: nil, result_metadata: nil},
     :hibernate}
  end

  ## Query helpers

  defp await_async_execution(state, 202, %{"statementHandle" => statement_handle}) do
    url = "/api/v2/statements/#{statement_handle}"

    req_client = build_req_client(state)

    case Req.get(req_client, url: url, receive_timeout: state.timeout) do
      {:ok, %{status: 202, body: body}} ->
        Process.sleep(state.async_poll_interval)
        await_async_execution(state, 202, body)

      {:ok, %{status: status, body: body}} when status in 200..299 ->
        {:ok, body}

      {:ok, %{body: %{"code" => code, "message" => message}}} ->
        {:error, %Error{message: String.replace(message, ~r/\n/, " "), code: code}}
    end
  end

  defp await_async_execution(_state, _status, body), do: {:ok, body}

  defp gather_results(state, %{"statementHandles" => statement_handles}, opts) do
    max_concurrency = System.schedulers_online()
    extended_timeout = opts[:timeout] + :timer.seconds(30)

    Task.Supervisor.async_stream_nolink(
      Snowflex.TaskSupervisor,
      statement_handles,
      fn handle ->
        case await_async_execution(state, 202, %{"statementHandle" => handle}) do
          {:ok, body} -> gather_results(state, body, opts)
          {:error, error} -> {:error, error}
        end
      end,
      max_concurrency: max_concurrency,
      ordered: true,
      timeout: extended_timeout,
      on_timeout: :kill_task
    )
    |> Enum.reduce_while({:ok, []}, fn
      {:ok, {:ok, result_body}}, {:ok, acc} ->
        {:cont, {:ok, [{:ok, result_body} | acc]}}

      {:ok, {:error, error}}, _acc ->
        {:halt, {:error, error}}

      {:exit, reason}, _acc ->
        {:halt, {:error, %Error{message: "Task failed: #{inspect(reason)}"}}}
    end)
    |> then(fn
      {:ok, results} -> {:ok, Enum.reverse(results)}
      error -> error
    end)
  end

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

    # Extended timeout to account for Req retries
    extended_timeout = opts[:timeout] + :timer.seconds(30)

    # Fetch partitions in parallel
    Task.Supervisor.async_stream_nolink(
      Snowflex.TaskSupervisor,
      rest_partitions,
      fn partition_index ->
        fetch_partition(state, statement_handle, partition_index, opts)
      end,
      max_concurrency: max_concurrency,
      ordered: true,
      timeout: extended_timeout,
      on_timeout: :kill_task
    )
    |> Enum.reduce_while({:ok, [initial_data]}, fn
      {:ok, {:ok, partition_body}}, {:ok, acc_chunks} ->
        partition_data = Map.get(partition_body, "data", [])
        {:cont, {:ok, [partition_data | acc_chunks]}}

      {:ok, {:error, error}}, _acc ->
        {:halt, {:error, error}}

      {:exit, reason}, _acc ->
        {:halt, {:error, %Error{message: "Task failed: #{inspect(reason)}"}}}
    end)
    |> then(fn
      {:ok, chunks} ->
        merged_data = chunks |> Enum.reverse() |> Enum.concat()
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
  defp validate_and_read_private_key(opts) do
    with {:ok, opts} <- validate_required_opts(opts),
         {:ok, opts, private_key} <- validate_and_read_private_key_opts(opts) do
      {:ok, opts, private_key}
    else
      {:stop, error} -> {:stop, error}
    end
  end

  defp validate_required_opts(opts) do
    Enum.reduce_while(
      [:account_name, :username],
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

  defp validate_and_read_private_key_opts(opts) do
    private_key_path = Keyword.get(opts, :private_key_path)
    private_key_from_string = Keyword.get(opts, :private_key_from_string)

    case {private_key_path, private_key_from_string} do
      {path, nil} when is_binary(path) and byte_size(path) > 0 ->
        read_private_key_from_file(opts, path)

      {nil, key} when is_binary(key) and byte_size(key) > 0 ->
        {:ok, opts, key}

      {path, key} when is_binary(path) and is_binary(key) ->
        {:stop,
         %Error{
           message: "Both :private_key_path and :private_key_from_string provided. Use only one."
         }}

      _any ->
        {:stop,
         %Error{message: "Either :private_key_path or :private_key_from_string must be provided"}}
    end
  end

  defp read_private_key_from_file(opts, path) do
    case File.read(path) do
      {:ok, key} ->
        {:ok, opts, key}

      {:error, reason} ->
        {:stop, %Error{message: "Failed to read private key from path: #{inspect(reason)}"}}
    end
  end

  defp init_state(validated_opts, private_key) do
    {:ok,
     refresh_token(%State{
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
       role: Keyword.get(validated_opts, :role),
       async_poll_interval: Keyword.get(validated_opts, :async_poll_interval, 1000),
       max_retries: Keyword.get(validated_opts, :max_retries, 3),
       retry_base_delay: Keyword.get(validated_opts, :retry_base_delay, 1000),
       retry_max_delay: Keyword.get(validated_opts, :retry_max_delay, 8000),
       connect_options: Keyword.get(validated_opts, :connect_options, []),
       req_options: validated_opts |> Keyword.get(:req_options, []) |> normalize_finch_option()
     })}
  end

  # Req 0.7 deprecated setting `:finch` to a bare pool name in favor of
  # `finch: [name: pool]`; wrap the bare name so existing configs keep working
  # without triggering the deprecation warning.
  defp normalize_finch_option(req_options) do
    case Keyword.fetch(req_options, :finch) do
      {:ok, pool} when is_atom(pool) and not is_nil(pool) ->
        Keyword.put(req_options, :finch, name: pool)

      _other ->
        req_options
    end
  end

  # Use an explicitly-configured fingerprint when present (backward compatible);
  # otherwise derive it from the private key so callers no longer need to supply it.
  defp resolve_fingerprint(validated_opts, private_key) do
    case fingerprint_for(validated_opts, private_key) do
      {:ok, fingerprint} ->
        {:ok, Keyword.put(validated_opts, :public_key_fingerprint, fingerprint)}

      {:error, reason} ->
        {:stop,
         %Error{
           message:
             "Failed to compute public key fingerprint: #{KeyFingerprint.error_message(reason)}"
         }}
    end
  end

  defp fingerprint_for(validated_opts, private_key) do
    case Keyword.get(validated_opts, :public_key_fingerprint) do
      fingerprint when is_binary(fingerprint) and byte_size(fingerprint) > 0 ->
        {:ok, fingerprint}

      _absent ->
        KeyFingerprint.fingerprint(
          private_key,
          Keyword.get(validated_opts, :private_key_password, ~c"")
        )
    end
  end

  defp check_connection(state) do
    case fetch_statement(state, "SELECT 1", %{}, timeout: state.timeout) do
      {:ok, _status, _body} ->
        {:ok, state}

      {:error, error} ->
        {:stop, error}
    end
  end

  # Token helpers

  # Signing a JWT costs a PEM decode plus an RSA private-key operation, so the
  # token is cached for its lifetime instead of being rebuilt per request. This
  # matters most for async polling, where one logical workflow issues many
  # requests. Callers that cannot update the GenServer state (the parallel
  # partition fetches, and the public options/1) reuse whatever is cached.
  defp refresh_token(%State{} = state) do
    now = System.system_time(:millisecond)

    if is_nil(state.auth_token) or now >= state.auth_token_expires_at - @token_refresh_margin do
      %{
        state
        | auth_token: generate_token(state),
          auth_token_expires_at: now + token_lifetime(state)
      }
    else
      state
    end
  end

  defp token_lifetime(%State{token_lifetime: lifetime}), do: lifetime

  defp generate_token(state) do
    now = System.system_time(:second)
    # :token_lifetime is configured in milliseconds, but a JWT `exp` claim is a
    # POSIX timestamp in seconds.
    expires_at = now + div(token_lifetime(state), 1000)

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

    token
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

  defp build_options(state) do
    base_url = "https://#{state.account_name}.snowflakecomputing.com"

    base_options = [
      base_url: base_url,
      headers: [
        {"Authorization", "Bearer #{state.auth_token || generate_token(state)}"},
        {"Content-Type", "application/json"},
        {"Accept", "application/json"},
        {"User-Agent", "snowflex/#{snowflex_version()}"},
        {"X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT"}
      ],
      retry: :safe_transient,
      retry_delay: fn attempt ->
        calculate_backoff_delay(attempt, state.retry_base_delay, state.retry_max_delay)
      end,
      max_retries: state.max_retries,
      compressed: true
    ]

    base_options
    |> maybe_put_connect_options(state)
    |> Keyword.merge(state.req_options)
  end

  # Req raises if both `:finch` and `:connect_options` are set. When the caller
  # supplies a `:finch` instance through `:req_options` (to route requests at a
  # dedicated, explicitly-sized Finch pool), omit `:connect_options` and let the
  # pool own its connection configuration.
  defp maybe_put_connect_options(options, %State{req_options: req_options} = state) do
    if Keyword.has_key?(req_options, :finch) do
      options
    else
      Keyword.put(options, :connect_options, state.connect_options)
    end
  end

  defp build_req_client(state) do
    state
    |> build_options()
    |> Req.new()
  end

  defp snowflex_version do
    Application.spec(:snowflex)[:vsn]
  end

  defp calculate_backoff_delay(attempt, base_delay, max_delay) do
    # Exponential backoff with jitter
    # attempt starts at 0, so we use attempt for the power calculation
    exponential_delay = base_delay * :math.pow(2, attempt)
    capped_delay = min(exponential_delay, max_delay)
    jitter = :rand.uniform() * 0.1 * capped_delay
    trunc(capped_delay + jitter)
  end

  defp format_response_body(body) when is_list(body) do
    Enum.map(body, fn {:ok, statement_body} -> format_response_body(statement_body) end)
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
        result(body, %{columns: [], rows: nil, num_rows: num_rows})

      %{
        "resultSetMetaData" => %{
          "rowType" => [%{"name" => "number of rows deleted"}]
        },
        "stats" => %{
          "numRowsDeleted" => num_rows
        }
      } ->
        result(body, %{columns: [], rows: nil, num_rows: num_rows})

      %{"data" => data, "resultSetMetaData" => metadata} ->
        columns = Enum.map(metadata["rowType"], & &1["name"])
        rows = data

        result(body, %{
          columns: columns,
          rows: rows,
          num_rows: length(rows),
          metadata: metadata
        })

      # Calls to additional partitions will not bring back the result set metadata
      %{"data" => data} ->
        result(body, %{rows: data, num_rows: length(data)})

      _any ->
        result(body, %{messages: [body["message"] || "Query executed successfully"]})
    end
  end

  defp result(body, attrs) do
    %{
      query_id: body["statementHandle"],
      request_id: body["requestId"],
      sql_state: body["sqlState"]
    }
    |> Map.merge(attrs)
    |> then(&struct!(Result, &1))
  end

  # HTTP Calls

  defp fetch_statement(state, statement, params, opts, call_opts \\ []) do
    req_body = %{
      statement: statement,
      timeout: statement_timeout_seconds(opts[:statement_timeout] || opts[:timeout]),
      database: state.database,
      schema: state.schema,
      warehouse: state.warehouse,
      role: state.role,
      bindings: params_to_bindings(params),
      parameters: request_params(opts)
    }

    url = "/api/v2/statements"

    req_client = build_req_client(state)

    # `async=true` makes Snowflake acknowledge with 202 + statementHandle
    # immediately instead of holding the response open until the statement
    # finishes (or until its own ~45s cutoff). Req treats an empty *list* as
    # "no params" and leaves the URL untouched; an empty map would take the
    # encode path instead.
    query_params = if call_opts[:async?], do: %{async: true}, else: []

    case Req.post(req_client,
           url: url,
           json: req_body,
           params: query_params,
           receive_timeout: opts[:timeout]
         ) do
      {:ok, %{status: status, body: body}} when status in 200..299 ->
        {:ok, status, body}

      {:ok, %{body: %{"code" => code, "message" => message}} = response} ->
        {:error,
         %Error{
           message: String.replace(message, ~r/\n/, " "),
           code: code,
           sql_state: Map.get(response.body, "sqlState"),
           metadata: %{
             query_id: Map.get(response.body, "statementHandle"),
             statement: statement,
             request: req_body,
             response: response.body,
             opts: opts
           }
         }}

      {:ok, %{status: status, body: body}} ->
        {:error,
         %Error{
           code: status,
           message: inspect(body),
           metadata: %{
             query_id: if(is_map(body), do: Map.get(body, "statementHandle"), else: nil),
             statement: statement,
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
           metadata: %{statement: statement, request: req_body, opts: opts}
         }}
    end
  end

  defp fetch_partition(state, statement_handle, partition_index, opts) do
    url = "/api/v2/statements/#{statement_handle}"
    params = %{partition: partition_index}
    req_client = build_req_client(state)

    case Req.get(req_client, url: url, params: params, receive_timeout: opts[:timeout]) do
      {:ok, %{status: status, body: partition_body}} when status in 200..299 ->
        {:ok, partition_body}

      {:ok, %{status: status, body: error_body}} ->
        {:error,
         %Error{
           message: "HTTP #{status}: #{inspect(error_body)}",
           metadata: %{
             query_id: statement_handle,
             partition_index: partition_index
           }
         }}

      {:error, exception} ->
        Logger.warning("Failed to fetch partition #{partition_index}: #{inspect(exception)}")

        {:error,
         %Error{
           message: "Failed to fetch partition: #{inspect(exception)}",
           metadata: %{
             query_id: statement_handle,
             partition_index: partition_index
           }
         }}
    end
  end

  # Snowflake reports async statement state through the status code of a plain
  # GET on the handle: 202 while it is still running, 2xx once it succeeded, and
  # 422 when it finished with an error.
  defp request_statement_status(state, handle, opts) do
    case get_statement(state, handle, opts) do
      {:ok, :running, _body} -> {:ok, :running}
      {:ok, :succeeded, _body} -> {:ok, :succeeded}
      {:error, error} -> {:error, error}
    end
  end

  # Reuses the same partition-gathering and formatting the synchronous execute
  # path uses, so a handle's rows decode exactly like a normal query's.
  defp request_result(state, handle, opts) do
    with {:ok, :succeeded, body} <- get_statement(state, handle, opts),
         {:ok, raw_result} <- gather_results(state, body, opts) do
      {:ok, format_response_body(raw_result)}
    else
      {:ok, :running, _body} -> {:ok, :running}
      {:error, error} -> {:error, error}
    end
  end

  defp get_statement(state, handle, opts) do
    req_client = build_req_client(state)

    # Snowflake keeps answering 202 for as long as the statement runs, so a
    # transient-retry here would burn the caller's whole timeout budget
    # re-asking a question that was already answered. Ask exactly once.
    case Req.get(req_client,
           url: statement_url(handle),
           receive_timeout: opts[:timeout],
           retry: false
         ) do
      {:ok, %{status: 202, body: body}} -> {:ok, :running, body}
      {:ok, %{status: status, body: body}} when status in 200..299 -> {:ok, :succeeded, body}
      {:ok, response} -> {:error, statement_error(response, handle, opts)}
      {:error, exception} -> {:error, transport_error(exception, handle, opts)}
    end
  end

  defp request_cancel(state, handle, opts) do
    req_client = build_req_client(state)

    case Req.post(req_client,
           url: statement_url(handle) <> "/cancel",
           receive_timeout: opts[:timeout]
         ) do
      {:ok, %{status: status, body: body}} when status in 200..299 ->
        {:ok, handle_result(body, handle)}

      {:ok, response} ->
        {:error, statement_error(response, handle, opts)}

      {:error, exception} ->
        {:error, transport_error(exception, handle, opts)}
    end
  end

  defp statement_url(handle), do: "/api/v2/statements/#{handle}"

  # Snowflake's `timeout` request field is a number of SECONDS, while every
  # Snowflex timeout option is in milliseconds. Passing the millisecond value
  # through told Snowflake to wait ~12.5 hours for the default 45_000, which
  # effectively disabled the server-side statement timeout. Round up so a
  # sub-second timeout does not become 0, which Snowflake reads as "no timeout".
  defp statement_timeout_seconds(:infinity), do: 0
  defp statement_timeout_seconds(nil), do: nil

  defp statement_timeout_seconds(timeout) when is_integer(timeout) and timeout > 0 do
    max(div(timeout + 999, 1000), 1)
  end

  defp statement_timeout_seconds(timeout), do: timeout

  # Handle-addressed acknowledgements (submit, cancel) carry no rows, and their
  # body is not guaranteed to be a JSON object, so build the Result from the
  # handle we already know instead of reading it back out of the body.
  defp handle_result(body, handle) do
    body = normalize_body(body)

    %Result{
      query_id: handle,
      request_id: body["requestId"],
      sql_state: body["sqlState"],
      rows: nil,
      num_rows: 0
    }
  end

  defp normalize_body(body) when is_map(body), do: body
  defp normalize_body(_body), do: %{}

  defp statement_error(%{status: status, body: body}, handle, opts) do
    case body do
      %{"code" => code, "message" => message} ->
        %Error{
          message: String.replace(message, ~r/\n/, " "),
          code: to_string(code),
          sql_state: Map.get(body, "sqlState"),
          metadata: %{query_id: handle, statement: handle, response: body, opts: opts}
        }

      _any ->
        %Error{
          message: "HTTP #{status}: #{inspect(body)}",
          code: to_string(status),
          metadata: %{query_id: handle, statement: handle, response: body, opts: opts}
        }
    end
  end

  defp transport_error(exception, handle, opts) do
    %Error{
      message: inspect(exception),
      code: "HTTP_ERROR",
      metadata: %{query_id: handle, statement: handle, opts: opts}
    }
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
