defmodule Snowflex.Transport.Http.KeyFingerprint do
  @moduledoc """
  Computes the Snowflake key-pair-auth public-key fingerprint from an RSA
  private (or public) key PEM.

  Snowflake stores this value per user as `RSA_PUBLIC_KEY_FP` (visible via
  `DESC USER <name>;`). It is defined as:

      base64(sha256(DER-encoded SubjectPublicKeyInfo of the RSA public key))

  and reproduces this OpenSSL pipeline:

      openssl rsa -in rsa_key.p8 -pubout -outform DER \\
        | openssl dgst -sha256 -binary \\
        | openssl base64

  ## Notes

    * It is the **SubjectPublicKeyInfo (SPKI)**, not PKCS#1, that Snowflake hashes
      (`-----BEGIN PUBLIC KEY-----`, the RSA key wrapped in an `AlgorithmIdentifier`).
      Hashing the bare PKCS#1 key would yield a plausible-but-wrong fingerprint.
    * Snowflake key-pair auth is **RSA only**. EC/DSA keys surface loudly as
      `{:error, {:unsupported_key, _}}`.
  """

  @rsa_key_requirement "Snowflake key-pair authentication requires an RSA private key PEM or " <>
                         "RSA SubjectPublicKeyInfo public key PEM"

  @doc """
  Computes the bare-base64 Snowflake public-key fingerprint from a PEM.

  Accepts a private key PEM (derives the public key) or a public key PEM (SPKI).
  `password` is the charlist password for an encrypted private key; pass `~c""`
  (the default) for an unencrypted key.
  """
  @spec fingerprint(binary(), charlist() | binary()) :: {:ok, String.t()} | {:error, term()}
  def fingerprint(pem, password \\ ~c"") when is_binary(pem) do
    with {:ok, entry} <- first_pem_entry(pem),
         {:ok, decoded} <- decode_entry(entry, password),
         {:ok, pub} <- to_rsa_public_key(decoded) do
      {:ok, fingerprint_from_public_key(pub)}
    end
  end

  @doc """
  Like `fingerprint/2`, but returns the fingerprint string or raises.
  """
  @spec fingerprint!(binary(), charlist() | binary()) :: String.t()
  def fingerprint!(pem, password \\ ~c"") do
    case fingerprint(pem, password) do
      {:ok, fp} ->
        fp

      {:error, reason} ->
        raise ArgumentError, "could not compute public key fingerprint: #{error_message(reason)}"
    end
  end

  @spec error_message(term()) :: String.t()
  def error_message(:empty_pem), do: "PEM input did not contain a key entry"

  def error_message({:decode_failed, exception}) do
    "could not decode PEM key entry (#{inspect(exception)}); " <>
      "verify the PEM is valid and the private_key_password is correct for encrypted keys"
  end

  def error_message({:unsupported_key, key_type}) when is_atom(key_type) do
    "unsupported key type #{Atom.to_string(key_type)}; #{@rsa_key_requirement}"
  end

  def error_message(_reason), do: "unexpected fingerprint error"

  defp first_pem_entry(pem) do
    case :public_key.pem_decode(pem) do
      [entry | _] -> {:ok, entry}
      [] -> {:error, :empty_pem}
    end
  end

  defp fingerprint_from_public_key(pub) do
    der = elem(:public_key.pem_entry_encode(:SubjectPublicKeyInfo, pub), 1)
    Base.encode64(:crypto.hash(:sha256, der))
  end

  # An empty password means an unencrypted key. Decode without the password arg:
  # passing a password to `pem_entry_decode/2` skips the SPKI/PKCS#8 unwrapping
  # that arity-1 performs, so a public-key (SPKI) PEM would not unwrap to an
  # :RSAPublicKey. A real password is only needed for encrypted private keys.
  defp decode_entry(entry, password) when password in [~c"", "", nil] do
    {:ok, :public_key.pem_entry_decode(entry)}
  rescue
    e -> {:error, {:decode_failed, e.__struct__}}
  end

  defp decode_entry(entry, password) do
    {:ok, :public_key.pem_entry_decode(entry, password)}
  rescue
    e -> {:error, {:decode_failed, e.__struct__}}
  end

  # RSA private key -> public components (modulus + public exponent).
  defp to_rsa_public_key({:RSAPrivateKey, _v, m, e, _d, _p, _q, _e1, _e2, _c, _o}),
    do: {:ok, {:RSAPublicKey, m, e}}

  # Already a public key (an SPKI PEM decodes straight to this).
  defp to_rsa_public_key({:RSAPublicKey, _m, _e} = pub), do: {:ok, pub}

  # Anything else (EC/DSA records, un-unwrapped PKCS#8, etc.) is unsupported.
  defp to_rsa_public_key(other), do: {:error, {:unsupported_key, key_type(other)}}

  defp key_type(tuple) when is_tuple(tuple) and tuple_size(tuple) > 0 do
    case elem(tuple, 0) do
      key_type when is_atom(key_type) -> key_type
      _other -> :unknown
    end
  end

  defp key_type(_other), do: :unknown
end
