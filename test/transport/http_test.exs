defmodule Snowflex.Transport.HttpTest do
  use ExUnit.Case

  alias Snowflex.Error
  alias Snowflex.Transport.Http

  defmodule DummyHttp do
    use GenServer

    @spec start_link(any()) :: GenServer.on_start()
    def start_link(_) do
      GenServer.start_link(__MODULE__, %{})
    end

    @impl GenServer
    def init(state) do
      {:ok, state}
    end

    @impl GenServer
    def handle_call({:execute, _statement, _params, _opts}, _from, state) do
      Process.sleep(50)
      {:reply, state, state}
    end
  end

  test "execute_statement/4 handles timeout" do
    {:ok, pid} = start_supervised(DummyHttp, %{})

    assert {:error, %Error{message: "Select 1 timed out after 10"}} =
             Http.execute_statement(pid, "Select 1", nil, timeout: 10)

    assert %{} = Http.execute_statement(pid, nil, nil, timeout: 1000)
  end

  describe "private key configuration" do
    # Sample private key for testing (this is a dummy key generated for testing only)
    @test_private_key """
-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCi4XQYD6ZG/kwc
b0hGEvbt44MQo69MU3kT2Q/6oWtoLaXb9eQ7VUs+0tRdNvgE36cRfKJNaJSYETG9
1GB38SPqNxgPhJROaQBhqOmlWFjOX4FAgk52U8U51tgwlgQCeLORsFqBYv2fGbwF
orwkyYsu92AGdMi0ejdRNtPNoBdhnOYUfi5KusTDmpPsfN31mT4y3gBQe8yLED/B
ZigzimlQW1DCjHHjOSYRcwRymOL+I9F7DX/KZFVLeHR3bL6ypk8msmoD5nqPv3W3
wTXLpdQUiQXySDPt466TmsweJbvi0d+Qhqhaykpk2zu0SDmWCo3MdyBSa0eAzBEI
gkc/58ddAgMBAAECggEAJ5ifXGoRhhiz8AWtkDt0BAjXB+iC6Q8x0/1kwQ0Uy1Kt
i3ePcE0f6bnfHnoKeUTVnI6r9h2CYiVr7jX+7amVjY6vLraQRy+HhDZH9oYvsJvP
FBuZb3KV86WjEMynVOJ65OP2XJXwCgl7h/Mzwc2tJFHG6krhr942LGwjuU094eCb
GFBf6sRxHzo84GHdH6FUdW+Rbz9idia8xCdGMYqgqPGdoPnFMFdg5nSc9VWv18su
b0acT4FPl5jgAUxQACB/sPbOeepWpaQribKyEXoUskzKoAyTjafquHlvTgbjEBCW
r12vlHlUVxvDnlDn8rsZyfjF28tGFSMKHGklsYRcAQKBgQDa6Ibbn1b9f+SMY7G6
Gi79W7sWAlhAYVHHC5Z99Yg1+YzwuUvymAGVOIeLZUCrBx7GWsaFswoYbbBbeTip
WlDAEw1WJPiV4UiEgaZUGLuD2TWMJ0mXHr+8UomynGl7noP9zRsJDIQcB9RtT90S
ILA/W2F80OoymjFEx03smBZnwQKBgQC+eqUdJMvGLVqyEr50PSKWV/B6nhTdowIa
skaIW264+Em8/22EmDUKCXxrEtRgHKKa/oRdS4QoiprKm+y4JFRGvvT6aDQigzpQ
TjxfKEaTIkzGx1gF3CHRucTwglfGSw76gl64ZuBLiU8iTLF/MnWtrWVXE3NMCmMF
Vo4cI6OmnQKBgQDTj0/t5uNiYPyXNS4pRm7NSp7XWXLC3Yr0C93oY6e9Si0M5Hdi
v8cf2J8ed790yo/ScR5VTj/edfuvm8rH2NIbnw4Ph/F6oFu+O7Jsqe5nMT+P8NQt
KXlx3m7XYFSNNRgo69VJ/H+cu3BwKHPlthO/V2gzAZClUOF2sAs+MYnNwQKBgQCY
wCnhMTzo9D4jR7zL4qr1/hevfU4mXy600fqWJxyn4RThJ8Vf69+86NaJ11PQr6YG
vczQNFsLV/vCN0Ciex/KjCBRH3ePpcUB2Xu4o8fU/lCrp/kC2gGU+nDgnuZc8pxU
cHAdWQLOEJMRYoeFBaYxXThDmCmB9WJrSXo1Pq9iUQKBgHCNHtVWRaDdg8Dr7DHX
OwKzb/z2D0JjA/efsPyBAU+9dCFItrW5O9VcbJlfUq5Vdgegv0a4KhKmGiXIuel8
XxtBHVc1SXQcY9WMNY3wS7ksL4nltXlMrqFJ5Y3qSj8y5wL3SxjOq5j1nYmXM9rq
EMnIHAkhdTGryeUSwyvEaZ/6
-----END PRIVATE KEY-----
"""

    @base_opts [
      account_name: "test-account",
      username: "test_user",
      public_key_fingerprint: "test_fingerprint"
    ]

    test "accepts private_key_path option" do
      # Create a temporary file with test private key
      temp_path = System.tmp_dir!() <> "/test_key.pem"
      File.write!(temp_path, @test_private_key)

      opts = @base_opts ++ [private_key_path: temp_path]

      # This will fail during connection check, but validation should pass
      # The error should be related to connection, not validation
      assert {:error, %Error{message: message}} = Http.start_link(opts)
      refute message =~ "Missing required option"
      refute message =~ "Either :private_key_path or :private_key must be provided"

      File.rm(temp_path)
    end

    test "accepts private_key_from_string option" do
      opts = @base_opts ++ [private_key_from_string: @test_private_key]

      # This will fail during connection check, but validation should pass
      # The error should be related to connection, not validation
      assert {:error, %Error{message: message}} = Http.start_link(opts)
      refute message =~ "Missing required option"
      refute message =~ "Either :private_key_path or :private_key_from_string must be provided"
    end

    test "rejects when both private_key_path and private_key_from_string are provided" do
      temp_path = System.tmp_dir!() <> "/test_key.pem"
      File.write!(temp_path, @test_private_key)

      opts = @base_opts ++ [private_key_path: temp_path, private_key_from_string: @test_private_key]

      assert {:error, %Error{message: "Both :private_key_path and :private_key_from_string provided. Use only one."}} =
               Http.start_link(opts)

      File.rm(temp_path)
    end

    test "rejects when neither private_key_path nor private_key_from_string are provided" do
      opts = @base_opts

      assert {:error, %Error{message: "Either :private_key_path or :private_key_from_string must be provided"}} =
               Http.start_link(opts)
    end

    test "rejects when private_key_path is empty string" do
      opts = @base_opts ++ [private_key_path: ""]

      assert {:error, %Error{message: "Either :private_key_path or :private_key_from_string must be provided"}} =
               Http.start_link(opts)
    end

    test "rejects when private_key_from_string is empty string" do
      opts = @base_opts ++ [private_key_from_string: ""]

      assert {:error, %Error{message: "Either :private_key_path or :private_key_from_string must be provided"}} =
               Http.start_link(opts)
    end

    test "rejects when private_key_path file does not exist" do
      opts = @base_opts ++ [private_key_path: "/nonexistent/path/key.pem"]

      assert {:error, %Error{message: message}} = Http.start_link(opts)
      assert message =~ "Failed to read private key from path"
    end
  end
end
