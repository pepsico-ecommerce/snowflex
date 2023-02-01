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
    [
      extra_applications: [:logger, :odbc],
      env: [
        driver: "/usr/lib/snowflake/odbc/lib/libSnowflake.so"
      ]
    ]
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
      {:poolboy, "~> 1.5.1"},
      {:backoff, "~> 1.1.6"},
      {:ecto, "~> 3.9"},
      {:ecto_sql, "~> 3.9"},
      {:db_connection, "~> 2.4"},
      {:telemetry, "~> 0.4 or ~> 1.0"},
      {:dialyxir, "~> 1.0", only: :dev, runtime: false},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false},
      {:meck, "~> 0.9", only: :test},
      {:ecto_sqlite3, "~> 0.8.2", only: [:dev, :test]}
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
