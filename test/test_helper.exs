require Snowflex.MigrationGenerator

opts = [strategy: :one_for_one, name: Snowflex.Supervisor]
Supervisor.start_link([Snowflex.SQLiteTestRepo], opts)

Snowflex.SQLiteTestRepo.__adapter__().storage_up(Snowflex.SQLiteTestRepo.config())

Snowflex.MigrationGenerator.generate_migrations(Snowflex.SQLiteTestRepo, [
  TestSchema,
  TestSchema2,
  TestSchema3
])

ExUnit.start()

ExUnit.after_suite(fn _ ->
  Snowflex.SQLiteTestRepo.__adapter__().storage_down(Snowflex.SQLiteTestRepo.config())
end)
