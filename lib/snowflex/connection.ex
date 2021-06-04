defmodule Snowflex.Connection do
  @moduledoc """
  Defines a Snowflake connection.

  ## Definition

  When used, the connection expects the `:otp_app` option. You may also define a standard timeout. This will default to 60 seconds.

  If `keep_alive?` is set to `true`, each worker in the connection pool will
  periodically send a dummy query to Snowflake to keep the authenticated
  session from expiring.

  ```
  defmodule SnowflakeConnection do
    use Snowflex.Connection,
      otp_app: :my_app,
      timeout: :timer.seconds(60),
      keep_alive?: true,
      map_nulls_to_nil?: true
  end
  ```

  Configuration should be extended in your config files.

  ```
  # config/prod.exs
  config :my_app, SnowflakeConnection,
    size: [
      max: 10,
      min: 5
    ],
    connection: [
        server: "snowflex.us-east-8.snowflakecomputing.com",
        role: "DEV",
        warehouse: "CUSTOMER_DEV_WH"
      ]
  ```

  The connection will default to using the `Snowflex.Worker` module. You are able to define a diferent one for testing/development purposes in your configurations as well.

  ```
  # config/dev.exs
    config :my_app, SnowflakeConnection,
      size: [
        max: 1,
        min: 1
      ],
      worker: MyApp.MockWorker
  ```

  ## Usage

  Ensure the connection is started as part of your application.

  ```
  defmodule MyApp.Application do

    def start(_, _) do
      ...

      children = [
        ...,
        SnowflakeConnection
      ]
    end
  end
  ```

  `execute/1`
  ```
  {:ok, query} = Snowflex.Query.create(%{query_string: "SELECT * FROM foo"})

  SnowflakeConnection.execute(query)

  # or with parameters

  query_string = \"""
    SELECT * FROM foo
    WHERE bar = ?
  \"""

  {:ok, query} = Snowflex.Query.create(%{query_string: query_string, params: ["baz"]})
  SnowflakeConnection.execute(query)
  ```

  You may also override any Connection-level options on each execute.

  ```
  {:ok, query} = Snowflex.Query.create(%{query_string: "SELECT * FROM foo"})

  SnowflakeConnection.execute(query, timeout: :timer.minutes(1))
  ```
  """

  @doc false
  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      @behaviour Snowflex.Connection

      # setup compile time config
      otp_app = Keyword.fetch!(opts, :otp_app)
      timeout = Keyword.get(opts, :timeout, :timer.seconds(60))
      map_nulls_to_nil? = Keyword.get(opts, :map_nulls_to_nil?, false)
      keep_alive? = Keyword.get(opts, :keep_alive?, false)

      @otp_app otp_app
      @name __MODULE__
      @default_size [
        max: 10,
        min: 5
      ]
      @keep_alive? keep_alive?
      @heartbeat_interval :timer.hours(3)
      @default_connection_opts [
        timeout: timeout,
        map_nulls_to_nil?: map_nulls_to_nil?
      ]

      def child_spec(_) do
        config = Application.get_env(@otp_app, __MODULE__, [])
        connection = Keyword.get(config, :connection, [])
        worker_module = Keyword.get(config, :worker, Snowflex.Worker)

        user_size_config = Keyword.get(config, :size, [])
        final_size_config = Keyword.merge(@default_size, user_size_config)

        min_pool_size = Keyword.get(final_size_config, :min)
        max_pool_size = Keyword.get(final_size_config, :max)

        opts = [
          {:name, {:local, @name}},
          {:worker_module, worker_module},
          {:size, max_pool_size},
          {:max_overflow, min_pool_size}
        ]

        :poolboy.child_spec(@name, opts,
          connection_args: connection,
          keep_alive?: @keep_alive?,
          heartbeat_interval: @heartbeat_interval
        )
      end

      @impl Snowflex.Connection
      def execute(query = %Snowflex.Query{}, connection_opts \\ []) do
        connection_opts = Keyword.merge(@default_connection_opts, connection_opts)
        Snowflex.do_query(@name, query, connection_opts)
      end
    end
  end

  ## Callbacks

  @doc """
  Wraps `Snowflex.do_query/3` and injects the relevant information from the connection
  """
  @callback execute(query :: Snowflex.Query.t(), connection_opts :: Snowflex.connection_opts()) ::
              Snowflex.sql_data() | {:error, any}
end
