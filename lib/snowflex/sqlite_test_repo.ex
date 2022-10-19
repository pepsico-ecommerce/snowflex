defmodule Snowflex.SQLiteTestRepo do
  use Ecto.Repo,
    otp_app: :snowflex,
    adapter: Ecto.Adapters.SQLite3
end
