defmodule Snowflex.MixProject do
  use Mix.Project

  def project do
    [
      app: :snowflex,
      version: "0.0.4",
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
    [
      extra_applications: extra_apps(Mix.env()),
      mod: {Snowflex.Application, []}
    ]
  end

  defp description do
    """
    The client interface for connecting to the Snowflake data warehouse.
    """
  end

  defp package do
    [
      # These are the default files included in the package
      files: ~w(lib .formatter.exs mix.exs README* LICENSE*),
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => "https://github.com/pepsico-ecommerce/snowflex"}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:poolboy, "~> 1.5.1"},
      {:backoff, "~> 1.1.6"},
      {:ecto, "~> 3.0"},
      {:ex_doc, "~> 0.21", only: :dev, runtime: false}
    ]
  end

  defp extra_apps(:prod) do
    [:logger, :odbc]
  end

  # credo:disable-for-lines:8
  defp extra_apps(_) do
    try do
      :odbc.module_info()
      [:logger, :odbc]
    rescue
      _e in UndefinedFunctionError -> [:logger]
    end
  end
end
