defmodule Snowflex.MixProject do
  use Mix.Project

  @source_url "https://github.com/pepsico-ecommerce/snowflex"
  @version "1.3.1"

  def project do
    [
      app: :snowflex,
      name: "Snowflex",
      version: @version,
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package(),
      assay: [
        dialyzer: [
          apps: [:project_plus_deps, :ex_unit, :mix],
          warning_apps: :project
        ]
      ],
      test_coverage: [tool: ExCoveralls],
      docs: docs(),
      elixirc_paths: elixirc_paths(Mix.env())
    ]
  end

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  def cli do
    [
      preferred_envs: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {Snowflex.Application, []}
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
      {:backoff, "~> 1.1.6"},
      # Ecto/DBConnection
      {:ecto, "~> 3.14"},
      {:ecto_sql, "~> 3.14"},
      {:db_connection, "~> 2.4"},
      # HTTP
      {:req, "~> 0.5"},
      {:plug, "~> 1.0"},
      {:jose, "~> 1.11"},
      {:jason, "~> 1.0"},
      # Linting
      {:assay, "~> 0.5", only: [:dev, :test], runtime: false},
      {:credo, "~> 1.7", only: :dev, runtime: false},
      {:excoveralls, "~> 0.13", only: :test},
      {:doctor, "~> 0.23.0", only: :dev},
      {:mix_audit, "~> 2.1", only: [:dev, :test], runtime: false},
      # Documentation
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false},
      # Runtime introspection
      {:telemetry, "~> 0.4 or ~> 1.0"}
    ]
  end

  defp docs do
    [
      extras: [
        "CHANGELOG.md": [],
        LICENSE: [title: "License"]
      ],
      main: "Snowflex",
      api_reference: false,
      source_url: @source_url,
      source_ref: "v#{@version}",
      formatters: ["html"]
    ]
  end
end
