defmodule Snowflex.Connection do
  @moduledoc """
  Defines a Snowflake connection.

  ## Definition

  When used, the connection expects the `:otp_app` option. You may also define a standard timeout. This will default to 60 seconds.

  ```
  defmodule SnowflakeConnection do
    use Snowflex.Connection,
      otp_app: :my_app,
      timeout: :timer.seconds(60)
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
  query = "SELECT * FROM foo"

  SnowflakeConnection.execute(query)
  ```

  `execute/2`
  ```
  query = \"""
    SELECT * FROM foo
    WHERE bar = ?
  \"""

  SnowflakeConnection.execute(query, [Snowflex.Query.string_param("baz")])
  ```
  """

  @doc false
  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      # setup compile time config
      otp_app = Keyword.fetch!(opts, :otp_app)
      timeout = Keyword.get(opts, :timeout, :timer.seconds(60))

      @otp_app otp_app
      @name __MODULE__
      @timeout timeout
      @default_size [
        max: 10,
        min: 5
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

        :poolboy.child_spec(@name, opts, connection)
      end

      def execute(query) when is_binary(query) do
        Snowflex.sql_query(@name, query, @timeout)
      end

      def execute(query, params) when is_binary(query) and is_list(params) do
        Snowflex.param_query(@name, query, params, @timeout)
      end
    end
  end
end
