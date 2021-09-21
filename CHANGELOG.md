# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.5] - 2021-09-21
### Fixed 
- Handle specific length byte strings raising exceptions when parsing database result, [closes #15](https://github.com/invokermain/pymssql-utils/issues/15).

## [0.1.4] - 2021-09-14
### Fixed 
- SQL Type `Uniqueidentifier` is now handled correctly and returned as a string, [closes #13](https://github.com/invokermain/pymssql-utils/issues/13).

## [0.1.3] - 2021-08-16
### Fixed 
- Fixed incorrect type hint for `DatabaseResult.data`.

### Changed
- Data parsing now raises warning when handling an unrecognised type
and returns identity function mapping instead of raising an Exception.
- This package now requires "pymssql>=2.1.4" instead of "pymssql>=2". This was implicit anyway as 2.1.4
is the minimum version that supports Python 3.7.

### Added
- Added a TDS Protocol version warning for users using version 7.2 or below, this is only checked once.
- Added some more type hints to `DatabaseResult`.

## [0.1.2] - 2021-07-14
### Fixed 
- Fix an issue where the returned result would be limited to 10,000 rows, #11.

## [0.1.1] - 2021-07-08
### Changed
- Relaxed Python version syntax (effectively the same versions, just a less explicit description).

## [0.1.0] - 2021-07-08
### Removed
- Dropped support for Python 3.6, this might still work, but it is not guaranteed or tested.

### Changed
- Improved intended type inference using `Cursor.description` information.
- Optimized performance by only checking the first non-null item in a column.
- Optimized memory usage by fetching and cleaning results in batches of 10k.
- Moved project and pipelines to use Poetry.
- Use flake8, isort and black for linting.
- Removed unnecessary dateutil dependency.

### Fixed
- Fixed an edge case where strings with a mix of non-datelike and datelike formats would be parsed as mixed data types instead of just string.