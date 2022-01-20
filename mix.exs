defmodule Snowflex.MixProject do
  use Mix.Project

  @source_url "https://github.com/pepsico-ecommerce/snowflex"
  @version "1.0.0"

  def project do
    [
      app: :snowflex,
      name: "Snowflex",
      version: @version,
      elixir: "~> 1.9",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package(),
      docs: docs(),
      elixirc_paths: elixirc_paths(Mix.env())
    ]
  end

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

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

  defp package do
    [
      description: "The client interface for connecting to the Snowflake data warehouse.",
      files: ~w(lib .formatter.exs mix.exs README* LICENSE* CHANGELOG*),
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => @source_url
      }
    ]
  end

  defp deps do
    [
      {:backoff, "~> 1.1.6"},
      {:ecto, "~> 3.12", optional: true},
      {:ecto_sql, "~> 3.12", optional: true},
      {:db_connection, "~> 2.4"},
      {:telemetry, "~> 0.4 or ~> 1.0"},
      {:dialyxir, "~> 1.0", only: :dev, runtime: false},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false},
      {:ecto_sqlite3, "~> 0.8.2", optional: true, only: [:dev, :test]},
      {:meck, "~> 0.9", only: :test},
      {:tesla, "~> 1.4", optional: true},
      {:jason, "~> 1.3", optional: true},
      {:hackney, "~> 1.18", optional: true}
    ]
  end

  defp docs do
    [
      extras: [
        "CHANGELOG.md": [],
        LICENSE: [title: "License"],
        "README.md": [title: "Overview"]
      ],
      main: "readme",
      api_reference: false,
      source_url: @source_url,
      source_ref: "v#{@version}",
      formatters: ["html"]
    ]
  end
end
