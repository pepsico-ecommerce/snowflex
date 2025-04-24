defmodule Snowflex.MixProject do
  use Mix.Project

  @source_url "https://github.com/pepsico-ecommerce/snowflex"
  @version "1.0.0"

  def project do
    [
      app: :snowflex,
      name: "Snowflex",
      version: @version,
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package(),
      dialyzer: [
        plt_add_apps: [:ex_unit, :mix, :credo, :earmark],
        list_unused_filters: true,
        flags: [
          :no_opaque,
          :unknown,
          :unmatched_returns,
          :extra_return,
          :missing_return,
          :error_handling
        ],
        plt_file: {:no_warn, "priv/plts/project.plt"},
        plt_core_path: "priv/plts/core.plt"
      ],
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test
      ],
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
      {:ecto, "~> 3.12"},
      {:ecto_sql, "~> 3.12"},
      {:db_connection, "~> 2.4"},
      # HTTP
      {:req, "~> 0.5"},
      {:jose, "~> 1.11"},
      {:jason, "~> 1.0"},
      # Linting
      {:dialyxir, "~> 1.4", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: :dev, runtime: false},
      {:ex_check, "~> 0.12", only: [:dev, :test], runtime: false},
      {:excoveralls, "~> 0.13", only: :test},
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
