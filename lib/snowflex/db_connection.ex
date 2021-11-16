defmodule Snowflex.DBConnection do
  @moduledoc """
  Defines a Snowflake connection with DBConnection adapter.

  ## Definition

  When used, the connection expects the `:otp_app` option. You may also define a standard timeout.
  This will default to 60 seconds.

  If `keep_alive?` is set to `true`, each worker in the connection pool will
  periodically send a dummy query to Snowflake to keep the authenticated
  session from expiring.

  ```
  defmodule SnowflakeDBConnection do
    use Snowflex.DBConnection,
      otp_app: :my_app,
      timeout: :timer.seconds(60),
      map_nulls_to_nil?: true,
      keep_alive?: true
  end
  ```
  """

  alias Snowflex.DBConnection.{
    Protocol,
    Query
  }

  @doc false
  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      # setup compile time config
      otp_app = Keyword.fetch!(opts, :otp_app)
      timeout = Keyword.get(opts, :timeout, :timer.seconds(60))
      map_nulls_to_nil? = Keyword.get(opts, :map_nulls_to_nil?, false)
      keep_alive? = Keyword.get(opts, :keep_alive?, false)

      @otp_app otp_app
      @timeout timeout
      @name __MODULE__

      def child_spec(_) do
        config = Application.get_env(@otp_app, __MODULE__, [])
        connection = Keyword.get(config, :connection, [])

        opts =
          Keyword.merge(config,
            timeout: @timeout,
            connection: connection
          )

        DBConnection.child_spec(Protocol, opts)
      end

      def start_link(opts) do
        DBConnection.start_link(Protocol, opts)
      end

      def execute(statement) when is_binary(statement) do
        case prepare_execute("", statement, [], []) do
          {:ok, _query, result} -> {:ok, result}
          {:error, error} -> {:error, error}
        end
      end

      def execute(statement, params) when is_binary(statement) and is_list(params) do
        case prepare_execute("", statement, params) do
          {:ok, _query, result} -> {:ok, result}
          {:error, error} -> {:error, error}
        end
      end

      defp prepare_execute(name, statement, params \\ [], opts \\ []) do
        query = %Query{name: name, statement: statement}
        DBConnection.prepare_execute(@name, query, params, opts)
      end
    end
  end
end
