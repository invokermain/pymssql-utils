# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2022-01-21
### Added
- Added support for multiple result sets, you can move between result sets using the new
  `next_set()` and `previous_set()` methods on `DatabaseResult`. `DatabaseResult` class
  also has a new attribute `set_count` to tell you how many result sets the query
  returned.
- `DatabaseResult.to_json()` now has an optional `with_columns` parameter.
- Added some missing doc strings.
### Changed
- Tests are now run against Python 3.10 as well.


## [0.2.0] - 2022-01-21
### Added
- Added some more tests, coverage now at 92%.
### Changed
- BREAKING: DatabaseResult's `source_types`, `columns`, `raw_data` and `data`,
  attributes/properties now cannot be `None`, in order to decrease type ambiguity. To 
  facilitate this change, they will raise a ValueError if they are called when the 
  DatabaseResult instance errored, or fetch was false.
- The SQLParameter type hint (used in various places) is now just an alias for Any.
- Miscellaneous refactoring and type hinting/guarding throughout the library. The 
  library now passes a basic `pyright` check with no errors. 


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