# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.1] - 2025-08-25

### Bug Fixes

- Fix migration generator depending on `safe_concat` for building module names (#101)

### Enhancements

- Accept req `:connect_options` config (#104)

## [1.0] - 2025-04-24

This release is a ground up rewrite of the library.

We completely remove ODBC in favor of utilizing Snowflex's HTTP API, and remove support for password based authentication, in line with Snowflake's own approach.

We also now expose a fully Ecto/DBConnection compatible implementation.

For complete details, as well as some helpful migration instructions, see the README.

## [0.5.3] - 2023-10-23

### Added

- Handle casting maybe date types [#78](https://github.com/pepsico-ecommerce/snowflex/pull/78)

## [0.5.2] - 2022-11-18

### Added

- Add unicode_string_param function [#60](https://github.com/pepsico-ecommerce/snowflex/pull/60)

### Fixed

- ODBC error not always a string [#53](https://github.com/pepsico-ecommerce/snowflex/pull/53)

## [0.5.1] - 2022-05-10

### Added

- Handle UTF-16 return data from Snowflake [#38](https://github.com/pepsico-ecommerce/snowflex/pull/38)

## [0.5.0] - 2022-01-19

### Added

- General improvements, add DBConnection adapter [#32](https://github.com/pepsico-ecommerce/snowflex/pull/32)

## [0.4.5] - 2021-10-29

### Added

- Callback that calls gc after queries to try to shuffle binary strings off the heap. We've seen large memory savings in a real application. [#23](https://github.com/pepsico-ecommerce/snowflex/pull/31)

## [0.4.4] - 2021-10-27

### Fixed

- Relax dependency requirement on `telemetry`

## [0.4.3] - 2021-10-27

### Fixed

- Hot fix for possible memory leak [#27](https://github.com/pepsico-ecommerce/snowflex/pull/29)

## [0.4.2] - 2021-10-08

### Fixed

- Properly convert elixir nil to erlang :null in query params

## [0.4.1] - 2021-09-02

### Added

- Added [telemetry 1.0](https://github.com/beam-telemetry/telemetry)
- New telemetry events around queries

## [0.4.0] - 2021-06-10

### Added

- Enabled SQL `INSERT` statements.

## [0.3.3] - 2021-06-07

### Fixed

- Corrected spelling in type definition

## [0.3.2] - 2021-06-02

### Added

- Added `map_nulls_to_nil?` variable to connection configuration to allow conversion of `:null` values to `:nil` in snowflake query response

## [0.3.1] - 2021-03-10

### Fixed

- Initialize the worker with a propslist instead of a tuple.
- create copies of the `odbc` type definitions

## [0.3.0] - 2021-03-09

### Added

- Added the ability to keep connections alive through configuration on the `Snowflex.Connection` module.

## [0.2.2] - 2021-02-09

### Fixed

- make sure to follow `{:error, reason}` convention in all parts of the worker

## [0.2.1] - 2020-11-20

### Fixed

- fix bug where handle_info is missing argument

## [0.2.0] - 2020-11-18

### Added

- Added the ability to define a module that will use `Snowflex.Connection` to maintain connection information

### Breaking

- Remove `Snowflex.Query` as it was a duplicate
- Remove `Snowflex.ConnectionPool` in favor of new `Snowflex.Connection`

## [0.1.1] - 2020-11-17

### Changed

- Configure the worker module on the `Snowflex.ConnectionPool` and no longer on the application level.

## [0.1.0] - 2020-11-16

### Breaking

- Users must launch connection pools as part of their application's
  supervision tree using `Snowflex.ConnectionPool`, and all queries must specify
  which connection pool to use. No connection pool will be started by default.
