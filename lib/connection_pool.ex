defmodule Snowflex.ConnectionPool do
  @moduledoc """

  ```
  data_warehouse = [
    connection: [
      server: "snowflex.us-east-8.snowflakecomputing.com",
      role: "DEV",
      warehouse: "CUSTOMER_DEV_WH"
    ]
  ]
  {Snowflex.ConnectionPool, data_warehouse}
  ```

  This is a contrived example. In production scenarios, you most likely want to
  setup your configuration in the application configuration and then pull it in
  at runtime.

  For example, if you are using Mix Releases, you would have this in the
  `config/releases.exs` file:

  ```
  import Config

  # ...

  config :my_app, :data_warehouses,
    point_of_sale: [
      name: :point_of_sale,
      connection: [
        role: "PROD",
        warehouse: System.get_env("SNOWFLAKE_POS_WH"),
        uid: System.get_env("SNOWFLAKE_POS_UID"),
        pwd: System.get_env("SNOWFLAKE_POS_PWD")
      ]
    ],
    advertising: [
      name: :advertising,
      connection: [
        role: "PROD",
        warehouse: System.get_env("SNOWFLAKE_ADVERTISING_WH"),
        uid: System.get_env("SNOWFLAKE_ADVERTISING_UID"),
        pwd: System.get_env("SNOWFLAKE_ADVERTISING_PWD")
      ]
    ]
  ```

  Then, in your application module, you would source the configuration like this:

  ```
  def MyApp.Application do
    use Application

    def start(_type, _args) do

      warehouses = Application.get_env(:myapp, :data_warehouses)
      pos = Keyword.get(warehouses, :point_of_sale)
      advertising = Keyword.get(warehouses, :advertising)

      children = [
        {Snowflex.ConnectionPool, pos},
        {Snowflex.ConnectionPool, advertising}
      ]

      opts = [strategy: :one_for_one, name: MyApp.Supervisor]
      Supervisor.start_link(children, opts)
    end
  end
  ```

  ## Name

  The `:name` of the connection pool must be an atom.

  ## Worker Module

  The `:worker_module` configuration is used to specifiy a different worker for the pool. We recommend this be used to setup a mock worker for testing/development.

  ## Connection

  The `:connection` configuration contains details on how to connect to
  Snowflake

  ## Pool Sizing

  The `:size` configuration is used to control the size of the pool. It accepts
  the `:max` (default: `10`) and `:min` (default: `5`) configuration keys. The
  pool will never create more than `:max` connections, but it will always create
  at least `:min` connections.

  ```
  [
    max: 10,
    min: 5
  ]
  ```

  ## Driver

  In order to connect to Snowflake, you need to provide the ODBC-compliant
  driver. Currently, Snowflex expects to use a single driver for all
  connections, and the driver must be specified in the application
  configuration. The `:driver` configuration key must be set to the
  fully-qualified path for the driver, which should be a dynamic library
  (`.so`).

  ```
  config Snowflex,
    driver: "/usr/lib/snowflake/odbc/lib/libSnowflake.so"
  ```

  Follow the [installation
  instructions](https://docs.snowflake.com/en/user-guide/odbc.html) from the
  Snowflake documentation to install the ODBC driver appropriate for your
  system.
  """

  @default_size [
    max: 10,
    min: 5
  ]

  @spec child_spec(keyword) :: :poolboy.child_spec()
  def child_spec(config) do
    name = Keyword.fetch!(config, :name)
    connection = Keyword.get(config, :connection)
    user_size_config = Keyword.get(config, :size, [])
    final_size_config = Keyword.merge(@default_size, user_size_config)

    min_pool_size = Keyword.get(final_size_config, :min)
    max_pool_size = Keyword.get(final_size_config, :max)
    worker_module = Keyword.get(config, :worker_module, Snowflex.Worker)

    opts = [
      {:name, {:local, name}},
      {:worker_module, worker_module},
      {:size, max_pool_size},
      {:max_overflow, min_pool_size}
    ]

    :poolboy.child_spec(name, opts, connection)
  end
end
