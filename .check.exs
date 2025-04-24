[
  ## all available options with default values (see `mix check` docs for description)
  # parallel: true,
  # skipped: true,
  retry: false,

  ## list of tools (see `mix check` docs for defaults)
  tools: [
    {:formatter, command: "mix format --check-formatted"},
    {:compiler, command: "mix compile --warnings-as-errors"},
    {:credo, command: "mix credo --strict"},
    {:dialyzer, command: "mix dialyzer"},
    {:ex_unit, command: "mix test --exclude integration", env: %{"MIX_ENV" => "test"}},
    {:ex_coveralls,
     command: "mix coveralls.html --exclude integration", env: %{"MIX_ENV" => "test"}}
  ]
]
