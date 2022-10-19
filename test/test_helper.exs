require Snowflex.MigrationGenerator

Snowflex.MigrationGenerator.generate_migrations(Snowflex.SQLiteTestRepo, [
  TestSchema,
  TestSchema2,
  TestSchema3
])

ExUnit.start()
