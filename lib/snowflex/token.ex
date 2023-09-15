defmodule Snowflex.Token do
  use Joken.Config

  @impl true
  def token_config do
    account = System.get_env("ACCOUNT")
    |> get_account_identifier()
    |> String.upcase()

    # Use uppercase for the account identifier and user name.
    user = String.upcase(System.get_env("USER"))
    qualified_username = "#{account}.#{user}"

    # Get the current time in order to specify the time when the JWT was issued and the expiration time of the JWT.
    now = Joken.current_time()

    # Specify the length of time during which the JWT will be valid. You can specify at most 1 hour. Set to 59 minutes.
    lifetime = 59 * 60

    # NB: This should be either generated or fetched from Snowflake
    public_key_fp = "SHA256:#{System.get_env("FINGERPRINT")}"

    signer = Joken.Signer.create("RS256", %{"pem" => File.read!(System.get_env("PRIV_KEY_FILE")), "passphrase" => System.get_env("PRIV_KEY_FILE_PWD")})

    %{}
    # Set the issuer to the fully qualified username concatenated with the public key fingerprint (calculated in the  previous step).
    |> add_claim("iss", fn -> "#{qualified_username}.#{public_key_fp}" end)
    # Set the subject to the fully qualified username.
    |> add_claim("sub", fn -> qualified_username end)
    # Set the issue time to now.
    |> add_claim("iat", fn -> now end)
    # Set the expiration time, based on the lifetime specified for this object.
    |> add_claim("exp", fn -> now + lifetime end)
  end

  def get_account_identifier(account, acc \\ "") do
    if String.contains?(account, ".global") do
      account
    else
      get_local_account_identifier(account, acc)
    end
  end

  # Get the account identifier without the region, cloud provider, or subdomain.
  defp get_local_account_identifier("." <> _rest, acc), do: acc
  defp get_local_account_identifier("-" <> _rest, acc), do: acc
  defp get_local_account_identifier(<<char::utf8, rest::binary>>, acc), do: get_local_account_identifier(rest, acc <> <<char::utf8>>)
  defp get_local_account_identifier(_account_indentifier, acc), do: acc
end
