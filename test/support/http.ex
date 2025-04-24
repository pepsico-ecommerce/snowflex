defmodule Http do
  @moduledoc """
  Test Ecto repo for integration tests with Snowflex adapter.
  """
  use Ecto.Repo,
    otp_app: :snowflex,
    adapter: Snowflex
end
