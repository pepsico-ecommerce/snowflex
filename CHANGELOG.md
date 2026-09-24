# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.7.0] - 2026-09-24

### Breaking Changes

- The minimum supported Req version is now 0.7 (`{:req, "~> 0.7"}`, previously `"~> 0.5"`). Consumers pinned to an older Req should stay on snowflex 1.6.0 until they can upgrade.
- `Snowflex.Transport.Http` sent the `timeout` field of a statement request in milliseconds, but Snowflake defines it in seconds. The default 45-second timeout therefore asked Snowflake to wait 45,000 seconds (about 12.5 hours), effectively disabling the server-side statement timeout; a configured `:timeout` is now converted to seconds. **Statements that previously ran unbounded will now be cancelled by Snowflake at the configured timeout**, so review `:timeout` for any long-running statement before upgrading.

### Enhancements

- Add `Snowflex.submit_async/4`, `Snowflex.statement_status/3`, `Snowflex.fetch_result/3` and `Snowflex.cancel_statement/3` for fire-and-forget statement submission. `submit_async/4` sends Snowflake's `async=true` query parameter, so the API acknowledges with a statement handle as soon as it accepts the statement instead of holding the response open until the statement finishes. A connection is checked out only for the submission round-trip, so long-running statements no longer occupy a pool slot for their full duration. `fetch_result/3` returns the statement's rows once it completes (and `{:ok, :running}` before that, so it can be used to poll), decoded exactly as `Ecto.Repo.query/4` decodes them. Submitted statements are bounded only by Snowflake's statement timeout, since nothing on the client side waits on them. ([#197](https://github.com/pepsico-ecommerce/snowflex/issues/197))
- `Snowflex.Transport` gains four optional callbacks — `submit_async/4`, `statement_status/3`, `fetch_result/3` and `cancel_statement/3` — implemented by `Snowflex.Transport.Http`. They are optional, so existing custom transports continue to satisfy the behaviour without change; calling an async function on a transport that does not implement them returns a descriptive `Snowflex.Error` rather than raising `UndefinedFunctionError`.
- `Snowflex.Transport.Http` now caches its signed JWT for the configured `:token_lifetime` instead of re-signing on every request. Each request previously performed a PEM decode and an RSA private-key operation; this is most noticeable when polling an async statement, where one workflow issues many requests.

### Bug Fixes

- JWT `exp` claims were computed by adding a millisecond `:token_lifetime` to a second-precision timestamp, so tokens advertised an expiry roughly a week away instead of the configured lifetime (10 minutes by default).
- `Snowflex.Transport.Http` no longer triggers Req 0.7's deprecation warning ("setting `:finch` to a Finch pool name is deprecated, use `finch: [name: name]` instead") on every request when a dedicated Finch pool is routed via `req_options: [finch: MyFinch]`. The canonical form is now `req_options: [finch: [name: MyFinch]]`; the bare pool name is still accepted and wrapped automatically.

## [1.6.0] - 2026-07-13

### Enhancements

- `Ecto.Repo.stream/2` is now lazy: it drives the DBConnection cursor and fetches one Snowflake partition at a time instead of executing eagerly (previously it gathered the entire result set before producing the first row). Because the connection must stay checked out while the stream is consumed and Snowflake has no transactions, the enumeration must run inside `Ecto.Repo.checkout/2` — mirroring ecto_sql's stream-inside-transaction requirement — and raises with instructions otherwise. Prefer `Repo.all/2` when you want the whole result set: the eager path fetches partitions in parallel. ([#188](https://github.com/pepsico-ecommerce/snowflex/pull/188))
- Add `Snowflex.stream_query/5`, the raw-SQL counterpart: it lazily streams a statement's result one `Snowflex.Result` partition at a time into a consumer function, checking out a connection for the duration of the function (or reusing the one held by an enclosing `checkout/2`). Streamed partitions carry the statement's `rowType` metadata, so values decode to the same types `execute` produces. ([#188](https://github.com/pepsico-ecommerce/snowflex/pull/188))
- The cursor halts with the final partition, so streams contain exactly one result per partition — no trailing empty result — and skip a needless final fetch round-trip. ([#188](https://github.com/pepsico-ecommerce/snowflex/pull/188))

### Bug Fixes

Repaired the DBConnection cursor path, which had never been exercisable ([#188](https://github.com/pepsico-ecommerce/snowflex/pull/188)):

- `Snowflex.Connection.handle_fetch/4` matched `{:cont, result}` from the transport, but transports return `{:ok, result}` per the `Snowflex.Transport` contract, so the first fetch of every cursor crashed with a `CaseClauseError`. Its `{:cont, ...}` branch also returned a `{:cont, result, query, state}` 4-tuple, which DBConnection rejects as a bad return.
- `Snowflex.Connection.handle_status/2` disconnected with "Snowflex does not support transactions", but `DBConnection.run/3` — the required wrapper for streaming, also used by Ecto's `checkout/2` — probes connection status around every checkout. It now reports `{:idle, state}` (a Snowflake connection is never inside a transaction). Explicit transaction callbacks (`handle_begin/commit/rollback`) still reject as before.
- `Snowflex.Transport.Http` `declare` did not poll async (HTTP 202) statement execution to completion the way `execute` does, so declaring a cursor for any statement running longer than ~45 seconds crashed the transport with a `CaseClauseError` instead of streaming its result.

## [1.5.0] - 2026-07-02

### Enhancements

- `Snowflex.Transport.Http` now derives the public-key fingerprint from your private key automatically, so `:public_key_fingerprint` is no longer required in config. The new `Snowflex.Transport.Http.KeyFingerprint` module computes `base64(sha256(DER SubjectPublicKeyInfo))` (matching Snowflake's `RSA_PUBLIC_KEY_FP`). An explicitly-provided `:public_key_fingerprint` is still honored. ([#184](https://github.com/pepsico-ecommerce/snowflex/pull/184))

## [1.4.1] - 2026-06-17

### Bug Fixes

- Enable gzip decompression for Snowflake API responses. Req v0.6.0 flipped the `compressed` option from `true` to `false` by default ([GHSA-655f-mp8p-96gv](https://github.com/wojtekmach/req/security/advisories/GHSA-655f-mp8p-96gv)), causing partition fetches to return raw gzip bytes instead of decoded JSON. `build_options/1` now explicitly sets `compressed: true`. ([#181](https://github.com/pepsico-ecommerce/snowflex/pull/181))

## [1.4.0] - 2026-06-16

### Enhancements

- Add support for `:map`, `{:map, inner_type}`, and `{:array, inner_type}` Ecto types stored as Snowflake `VARIANT` columns ([#172](https://github.com/pepsico-ecommerce/snowflex/pull/172), [#180](https://github.com/pepsico-ecommerce/snowflex/pull/180)). Values are automatically JSON-encoded via `PARSE_JSON(?)` on insert, update, and update_all, and decoded on read.

### Bug Fixes

- Recycle pool workers when their HTTP transport process dies. `Snowflex.Connection.connect/1` no longer calls `Process.flag(:trap_exit, true)`, so the link from worker → transport propagates death normally: when the transport crashes, the worker dies with it, the DBConnection pool starts a replacement, and the next query lands on a fresh transport instead of looping forever on `{:noproc, ...}` against a stale pid.

## [1.3.1] - 2026-06-10

### Enhancements

- Support routing Snowflake REST requests at a dedicated Finch pool. When a `:finch` instance is supplied via `:req_options`, `Snowflex.Transport.Http` now omits `:connect_options` so Req does not reject the request for setting both `:finch` and `:connect_options`. This lets callers point a warehouse at an explicitly-sized, isolated Finch pool. ([#174](https://github.com/pepsico-ecommerce/snowflex/pull/174))

### Changed

- Relax `ecto` and `ecto_sql` requirements from `~> 3.14` to `~> 3.13` so consumers can stay on the 3.13 series.

## [1.3.0] - 2026-06-08

### Enhancements

- Implement aggregate `filter()` via CASE WHEN translation ([#168](https://github.com/pepsico-ecommerce/snowflex/pull/168))
  - `count(x) |> filter(cond)`, `sum`, `avg`, `min`, `max` translate to `agg(CASE WHEN cond THEN x END)`
  - `count() |> filter(cond)` (COUNT(*)) maps to Snowflake's `COUNT_IF(cond)`

### Bug Fixes

- Raise a clear `ArgumentError` for unsupported `on_conflict` modes (`:nothing`, field list, update query) pointing users toward `MERGE INTO` ([#167](https://github.com/pepsico-ecommerce/snowflex/pull/167))

### Changed

- Bump minimum Elixir version to 1.15 ([#163](https://github.com/pepsico-ecommerce/snowflex/pull/163))
- Replace ex_check + dialyxir with assay for CI ([#166](https://github.com/pepsico-ecommerce/snowflex/pull/166))
- Bump ecto_sql and doctor, enabling plug and decimal upgrades to avoid CVE versions ([#165](https://github.com/pepsico-ecommerce/snowflex/pull/165))
- Bump req from 0.5.x to 0.6.0 ([#169](https://github.com/pepsico-ecommerce/snowflex/pull/169))
- Bump jose from 1.11.10 to 1.11.12 ([#128](https://github.com/pepsico-ecommerce/snowflex/pull/128))
- Bump ecto from 3.13.5 to 3.13.6 ([#153](https://github.com/pepsico-ecommerce/snowflex/pull/153))
- Bump ecto_sql from 3.13.4 to 3.13.5 ([#147](https://github.com/pepsico-ecommerce/snowflex/pull/147))
- Bump jason from 1.4.4 to 1.4.5 ([#157](https://github.com/pepsico-ecommerce/snowflex/pull/157))
- Bump telemetry from 1.3.0 to 1.4.1 ([#148](https://github.com/pepsico-ecommerce/snowflex/pull/148))
- Bump assay from 0.5.2 to 0.6.1 ([#170](https://github.com/pepsico-ecommerce/snowflex/pull/170))
- Bump credo from 1.7.16 to 1.7.19 ([#149](https://github.com/pepsico-ecommerce/snowflex/pull/149), [#151](https://github.com/pepsico-ecommerce/snowflex/pull/151), [#171](https://github.com/pepsico-ecommerce/snowflex/pull/171))
- Bump ex_doc from 0.40.0 to 0.40.3 ([#142](https://github.com/pepsico-ecommerce/snowflex/pull/142), [#159](https://github.com/pepsico-ecommerce/snowflex/pull/159))

## [1.2.1] - 2026-02-27

### Bug Fixes

- Dramatically improve memory usage for large datasets #145
- Fix broken hexdoc links #143 by @henriqueberlesi

### Changed

- Bump Credo from 1.7.15 to 1.7.16 #139 by dependabot

## [1.2.0] - 2026-02-09

### Enhancements

- Add support for `options/1` to get Req.options ([#140](https://github.com/pepsico-ecommerce/snowflex/pull/140))

### Bug Fixes

- Resolve failing integration tests ([#140](https://github.com/pepsico-ecommerce/snowflex/pull/140))
- Fix compiler warning on Elixir 1.19 ([#140](https://github.com/pepsico-ecommerce/snowflex/pull/140))
- Fix global setting of req options ([#140](https://github.com/pepsico-ecommerce/snowflex/pull/140))

### Changed

- Bump [ex_doc from 0.39.3 to 0.40.0](https://github.com/elixir-lang/ex_doc/compare/v0.39.3...v0.40.0) ([#138](https://github.com/pepsico-ecommerce/snowflex/pull/138)) by dependabot
- Bump [db_connection from 2.8.1 to 2.9.0](https://github.com/elixir-ecto/db_connection/compare/v2.8.1...v2.9.0) ([#137](https://github.com/pepsico-ecommerce/snowflex/pull/137)) by dependabot
- Bump [req from 0.5.16 to 0.5.17](https://github.com/wojtekmach/req/compare/v0.5.16...v0.5.17) ([#136](https://github.com/pepsico-ecommerce/snowflex/pull/136)) by dependabot
- Bump [credo from 1.7.14 to 1.7.15](https://github.com/rrrene/credo/compare/v1.7.14...v1.7.15) ([#135](https://github.com/pepsico-ecommerce/snowflex/pull/135)) by dependabot
- Bump [ex_doc from 0.39.2 to 0.39.3](https://github.com/elixir-lang/ex_doc/compare/v0.39.2...v0.39.3) ([#134](https://github.com/pepsico-ecommerce/snowflex/pull/134)) by dependabot
- Bump [plug from 1.19.0 to 1.19.1](https://github.com/elixir-plug/plug/compare/v1.19.0...v1.19.1) ([#133](https://github.com/pepsico-ecommerce/snowflex/pull/133)) by dependabot
- Bump [ecto_sql from 3.13.2 to 3.13.4](https://github.com/elixir-ecto/ecto_sql/compare/v3.13.2...v3.13.4) ([#132](https://github.com/pepsico-ecommerce/snowflex/pull/132)) by dependabot
- Bump [plug from 1.18.1 to 1.19.0](https://github.com/elixir-plug/plug/compare/v1.18.1...v1.19.0) ([#131](https://github.com/pepsico-ecommerce/snowflex/pull/131)) by dependabot
- Bump [ex_doc from 0.39.1 to 0.39.2](https://github.com/elixir-lang/ex_doc/compare/v0.39.1...v0.39.2) ([#130](https://github.com/pepsico-ecommerce/snowflex/pull/130)) by dependabot
- Bump [credo from 1.7.13 to 1.7.14](https://github.com/rrrene/credo/compare/v1.7.13...v1.7.14) ([#129](https://github.com/pepsico-ecommerce/snowflex/pull/129)) by dependabot

## [1.1.0] - 2025-10-28

### Enhancements

- Add support for passing in private keys as files _or_ as raw strings (#113) by @nyo16

### Changed

- Bump req from 0.5.15 to 0.5.16 (#126) by dependabot
- Bump dialyxir from 1.4.6 to 1.4.7 (#125) by dependabot
- Bump ecto from 3.13.4 to 3.13.5 (#124) by dependabot

## [1.0.5] - 2025-10-28

### Enhancements

- Add `ping/1` callback to `Snowflex.Transport` behavior to allow transports to opt out of periodic ping checks
- HTTP transport now skips unnecessary ping queries, eliminating rate limit errors (HTTP 429) from DBConnection's idle connection checks
- Add more detailed metadata into Logger.

## [1.0.4] - 2025-09-03

### Bug Fixes

- Change to using `identifier` instead of `literal` to match Ecto's changes
- Fix compiler warning in tests

#### Enhancements

- Automatically track application version in User-Agent header by introspecting Application spec.

## [1.0.3] - 2025-08-29

### Bug Fixes

- Add float_decode support for Decimal types to handle data type mismatches (#107)

## [1.0.2] - 2025-08-25

### Bug Fixes

- Handle timeouts from execute_statement in `http.ex` (#100)

### Changed

- Bump dialyxir from 1.4.5 to 1.4.6 (#102) by dependabot

- Bump ex_doc from 0.38.2 to 0.38.3 (#103) by dependabot

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
