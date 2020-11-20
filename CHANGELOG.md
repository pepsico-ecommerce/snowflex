# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
