defmodule Snowflex.MixProject do
  use Mix.Project

  def project do
    [
      app: :snowflex,
      version: "0.4.5",
      elixir: "~> 1.9",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: description(),
      package: package(),
      name: "Snowflex"
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    case Application.get_env(:snowflex, :transport, :odbc) do
      :odbc ->
        [
          extra_applications: [:logger, :odbc],
          env: [driver: "/usr/lib/snowflake/odbc/lib/libSnowflake.so"]
        ]

      :http ->
        [extra_applications: [:logger]]

      transport ->
        raise "unrecognized transport #{inspect(transport)} configured for :snowflex"
    end
  end

  defp description do
    """
    The client interface for connecting to the Snowflake data warehouse.
    """
  end

  defp package do
    [
      # These are the default files included in the package
      files: ~w(lib .formatter.exs mix.exs README* LICENSE* CHANGELOG*),
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => "https://github.com/pepsico-ecommerce/snowflex"}
    ]
  end

  defp deps do
    [
      {:poolboy, "~> 1.5.1"},
      {:backoff, "~> 1.1.6"},
      {:ecto, "~> 3.0", optional: true},
      {:db_connection, "~> 2.4"},
      {:telemetry, "~> 0.4 or ~> 1.0"},
      {:dialyxir, "~> 1.0", only: :dev, runtime: false},
      {:ex_doc, "~> 0.21", only: :dev, runtime: false},
      {:meck, "~> 0.9", only: :test},
      {:tesla, "~> 1.4", optional: true},
      {:jason, "~> 1.3", optional: true},
      {:hackney, "~> 1.18", optional: true}
    ]
  end
end
