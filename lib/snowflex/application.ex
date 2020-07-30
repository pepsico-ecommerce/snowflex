defmodule Snowflex.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  def start(_type, _args) do
    # List all child processes to be supervised
    children = [
      :poolboy.child_spec(:worker, poolboy_config())
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Snowflex.Supervisor]
    Supervisor.start_link(children, opts)
  end

  #  HELPERS

  defp poolboy_config do
    pool_config = Application.get_env(:snowflex, :pool)

    [
      {:name, {:local, :snowflex_pool}},
      {:worker_module, Application.get_env(:snowflex, :worker, Snowflex.Worker)},
      {:size, pool_config[:pool_size]},
      {:max_overflow, pool_config[:overflow]}
    ]
  end
end
