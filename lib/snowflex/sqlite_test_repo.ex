if Code.ensure_loaded?(Ecto.Adapters.SQLite3) do
  defmodule Snowflex.SQLiteTestRepo do
    use Ecto.Repo,
      otp_app: :snowflex,
      adapter: Ecto.Adapters.SQLite3
  end
end
