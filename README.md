# pymssql-utils (ALPHA)
A lightweight module that wraps and extends the [pymssql](https://github.com/pymssql/pymssql) library.

The aim of this module is to make it easier to use _pymssql_ by:
 * Reducing the amount of boilerplate code needed, and being fully type hinted.
 * Baking in sensible defaults and good usage patterns.
 * Returning a helpful `DatabaseResult` class for each execution.
 * Parsing various SQL Types to native Python types that _pymssql_ misses.
 * Making it easier to serialise your data.
 * Letting you easily choose how to handle errors.
 * Providing various utility functions for building dynamic SQL queries or logging query output.
 * Fixing various edge case bugs that arise when using _pymssql_.

The trade-off with "baking in" various patterns and options
is that this module is opinionated as to what best practice is. This module does not
strive to be the most performant or flexible way of using _pymssql_, for that just use
_pymssql_. However, this module aims to make interfacing with a MS SQL server to be
as hassle-free as possible, while making your code explicit and neat.

This module was created naturally over the course of a few years of using _pymssql_ in various projects.

Please raise any suggestions or issues via GitHub.

## Installation

This module can be installed via pip: `pip install pymssql-utils`.

This module requires `Python >= 3.6` and `Pip >= 19.3`

## Quick Start  (TODO)

Running a simple query:

```
import os

import pymssqlutils as sql

os.environ["DB_NAME"] = "MASTER"
```
## Notes

### Why _pymssql_ when Microsoft officially recommends _pyodbc_ (opinion)?

The main difference between _pyodbc_ and _pymssql_ is the drivers they use.
The ODBC are newer and have various levels of support on differing linux distributions,
and if you develop for containers or distribute code onto different platforms
you can run into ODBC driver-related issues that FreeTDS tends to not have.

There are other minor reasons someone might prefer _pymssql_, e.g.:
 * _pymssql's_ parameter subsitution is done client-side improving operation visibility.
 * _pymssql_ also has support for MSSQL specific data types such as Datetimeoffset.