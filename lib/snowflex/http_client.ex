defmodule Snowflex.HttpClient do
  @moduledoc """
  Tesla Client for [Snowflake SQL REST API](https://docs.snowflake.com/en/developer-guide/sql-api/index)
  """
  use Tesla

  alias Snowflex.Token

  plug Tesla.Middleware.BaseUrl, base_url()
  plug Tesla.Middleware.BearerAuth, token: jwt_token()
  plug Tesla.Middleware.Headers, [{"Accept", "*/*"}, {"X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT"}]
  plug Tesla.Middleware.JSON
  plug Tesla.Middleware.Logger

  # Config

  def base_url(), do: CDNA.get_config(:snowflake_client_base_url)
  def jwt_token() do
    signer = Joken.Signer.create("RS256", %{"pem" => File.read!(System.get_env("PRIV_KEY_FILE")), "passphrase" => System.get_env("PRIV_KEY_FILE_PWD")})
    Token.generate_and_sign!(%{}, signer)
  end

  # API

  def statements(params \\ nil, opts \\ []) do
    params = params || demo_params()

    post("/statements", params, opts)
  end

  def demo_params(statement \\ nil) do
    statement = statement || "SELECT COUNT(*) FROM CDP_DEV.INFO_MART.CDNA_AUDIENCE WHERE ((SEVENELEVEN IN (7, 8, 3)));"

    %{
      "statement": statement,
      "timeout": 60,
      "database": "CDP_DEV",
      "schema": "INFO_MART",
      "warehouse": "MA_PROD_CDPUI_WH",
      "role": "DP_CDNA_APP_ENGINEER"
    }
  end
end
