defmodule Snowflex.Application do
  @moduledoc false
  use Application

  @impl Application
  def start(_type, _args) do
    children = [
      {Task.Supervisor, name: Snowflex.TaskSupervisor}
    ]

    opts = [strategy: :one_for_one, name: Snowflex.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
