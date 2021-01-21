# pymssql-utils
A lightweight module that wraps and extends the [pymssql](https://github.com/pymssql/pymssql) library.

The aim of this module is to make it easier to use _pymssql_ by:
 * Reducing the amount of boilerplate code needed.
 * Returning a helpful `DatabaseResult` class for each execution.
 * Storing each row of data as a `Row` class which is a tuple that allows simple access via column names as well.
 * Being fully type hinted.  
 * Parsing various SQL Types to native Python types that Python misses.
 * Making it easier to serialise your data.
 * Providing various utility functions for building dynamic SQL queries or logging query output.
 * Fixing various edge case bugs that arise when using _pymssql_.

The trade-off with "baking in" various patterns (primarily opening a new connection on every execution)
is that this module is opinionated as to what best practice is,
and might not be suitable for all use cases. If this module doesn't make your life easier
please raise critiques or suggestions via Github.

This module was created naturally over the course of a few years of using _pymssql_ in various projects.

## Installation

This module can be installed via pip: `pip install pymssql-utils`.
This module requires Python >= 3.6

## Quick Start  (TODO)

Running a simple query:

```
import os

import pymssqlutils as sql

os.environ["DB_NAME"] = "MASTER"
```
## Notes

### Why _pymssql_ when Microsoft officially recommends _pyodbc_ (opinion)?

ODBC drivers add an extra layer between Python and SQL Server
which can introduce issues that outway the benefits of pyodbc.
The drivers have various levels of support on differeing linux distributions,
and if you develop in containers or require to run the same code on various platforms
you can run into issues. There are other minor reasons that I prefer _pymssql_, e.g. _pymssql's_
parameter subsitution is done client-side leading to better visibiltiy on what the server is actually running.
And _pymssql_ (tries to) supports DATETIMEOFFSET SQL type while _pyodbc_ does not.