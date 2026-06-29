defmodule Snowflex.Transport.Http.KeyFingerprintTest do
  use ExUnit.Case, async: true

  alias Snowflex.Transport.Http.KeyFingerprint

  # Committed throwaway key. Its fingerprint was computed with OpenSSL:
  #   openssl rsa -in fake_private_key.pem -pubout -outform DER \
  #     | openssl dgst -sha256 -binary | openssl base64
  @fixture_pem File.read!("test/fixtures/fake_private_key.pem")
  @fixture_fingerprint "Tf0scbc8DFpgDF6WAEylIgTf10Yy6OU3OHcFOPe4r1w="

  # The same throwaway key, re-encrypted as an AES-256 PKCS#8 blob with the
  # password "secret":
  #   openssl pkcs8 -topk8 -in fake_private_key.pem -v2 aes-256-cbc \
  #     -passout pass:secret -out fake_encrypted_private_key.pem
  # Decrypting it must recover the same public key, so its independently
  # OpenSSL-computed fingerprint equals @fixture_fingerprint.
  @encrypted_fixture_pem File.read!("test/fixtures/fake_encrypted_private_key.pem")
  @ec_pem """
  -----BEGIN PRIVATE KEY-----
  MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgzSgMujIQ2WrPZZ5m
  RoP0jJZGB3+k+ybFkc6Aj0ciC2ahRANCAAT2s0r05jC70ZwAVm29wklYY2RIK2Zb
  kHqeGxq9V7YXPUYl14/u0Rm3+eZjMpLbIqctuEq1Bj1g48W6QIEp14c+
  -----END PRIVATE KEY-----
  """

  describe "fingerprint/2" do
    test "computes the Snowflake fingerprint from a PKCS#8 private key PEM" do
      assert {:ok, @fixture_fingerprint} = KeyFingerprint.fingerprint(@fixture_pem)
    end

    test "returns the bare base64 (no SHA256: prefix), matching RSA_PUBLIC_KEY_FP minus prefix" do
      assert {:ok, fp} = KeyFingerprint.fingerprint(@fixture_pem)
      refute String.starts_with?(fp, "SHA256:")
    end

    test "derives the same fingerprint from a public key PEM (SPKI) as from the private key" do
      public_pem = derive_public_pem(@fixture_pem)
      assert {:ok, @fixture_fingerprint} = KeyFingerprint.fingerprint(public_pem)
    end

    test "errors on input that contains no PEM entry" do
      assert {:error, :empty_pem} = KeyFingerprint.fingerprint("not a pem")
    end

    test "computes the fingerprint from an encrypted RSA key with the correct password" do
      assert {:ok, @fixture_fingerprint} =
               KeyFingerprint.fingerprint(@encrypted_fixture_pem, ~c"secret")
    end

    test "returns a decode error (not :unsupported_key) for an encrypted key with the wrong password" do
      assert {:error, {:decode_failed, _}} =
               KeyFingerprint.fingerprint(@encrypted_fixture_pem, ~c"wrong")
    end

    test "errors on a non-RSA key without retaining decoded private key material" do
      # A real P-256 EC key — Snowflake keypair auth is RSA only, so this must
      # surface as :unsupported_key rather than producing a bogus fingerprint.
      assert {:error, {:unsupported_key, :ECPrivateKey} = reason} =
               KeyFingerprint.fingerprint(@ec_pem)

      refute inspect(reason) =~ "<<"
    end

    test "formats unsupported key errors with remediation guidance" do
      assert KeyFingerprint.error_message({:unsupported_key, :ECPrivateKey}) ==
               "unsupported key type ECPrivateKey; Snowflake key-pair authentication requires an RSA private key PEM or RSA SubjectPublicKeyInfo public key PEM"
    end
  end

  describe "fingerprint!/2" do
    test "returns the fingerprint string on success" do
      assert KeyFingerprint.fingerprint!(@fixture_pem) == @fixture_fingerprint
    end

    test "raises on invalid input" do
      assert_raise ArgumentError, fn -> KeyFingerprint.fingerprint!("not a pem") end
    end

    test "raises with a sanitized explanatory message for non-RSA keys" do
      error = assert_raise ArgumentError, fn -> KeyFingerprint.fingerprint!(@ec_pem) end

      assert Exception.message(error) =~ "unsupported key type ECPrivateKey"
      assert Exception.message(error) =~ "requires an RSA private key PEM"
      refute Exception.message(error) =~ "<<"
    end
  end

  # Build the SPKI public key PEM from the private key, mirroring
  #   openssl rsa -in key.p8 -pubout
  defp derive_public_pem(private_pem) do
    [entry] = :public_key.pem_decode(private_pem)

    {:RSAPrivateKey, _v, m, e, _d, _p, _q, _e1, _e2, _c, _o} =
      :public_key.pem_entry_decode(entry)

    pub = {:RSAPublicKey, m, e}
    pub_entry = :public_key.pem_entry_encode(:SubjectPublicKeyInfo, pub)
    :public_key.pem_encode([pub_entry])
  end
end
