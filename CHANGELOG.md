# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Configure the worker module on the `Snowflex.ConnectionPool`

- _Breaking_ Users must launch connection pools as part of their application's
  supervision tree using `Snowflex.ConnectionPool`, and all queries must specify
  which connection pool to use. No connection pool will be started by default.
