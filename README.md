# pymssql-utils (ALPHA)
_pymssql-utils_ is a small library that wraps
[pymssql](https://github.com/pymssql/pymssql) to make your life easier.
It provides a higher-level API, as well as a some utility methods,
so that you can think less about connections and cursors.

This module's features:
* Higher-level API that reduces the amount of boilerplate required.
* Baked-in sensible defaults and usage patterns.
* Provides optional execution batching, similar to
  [_pyodbc's_](https://github.com/mkleehammer/pyodbc) `fast_executemany`.
* Parses the SQL Types that _pymssql_ misses to native Python types, and vice versa.
* Makes it easy to serialize your data with
  [_orjson_](https://github.com/ijl/orjson).
* Provides you with simple and clear options for error handling.
* Extra utility functions, e.g. for building dynamic SQL queries.
* Fixing various edge case bugs that arise when using _pymssql_.
* Fully type hinted.

This module's enforced opinions (check these work for you):
* Each execution opens and closes a connection using _pymssql_'s
  context management.
* Execution data is returned as a dictionary, as accessing data by column name
  is clearer and simpler than by index.
* Converts numeric data to `float` as this is easier to work with than `Decimal`
  and for the vast majority of cases 'good enough'.
  
When you shouldn't use this module:
* If you need fine-grained control over your cursors.
* If performance is a must (use [_pyodbc's_](https://github.com/mkleehammer/pyodbc))
  
Please raise any suggestions or issues via GitHub.

## Usage
### Installation

This library can be installed via pip: `pip install --upgrade pymssql-utils`.
This library requires `Python >= 3.6` and `Pip >= 19.3`.

### Quickstart

This library provides two high-level methods:
 * `Query`: non-committing, fetches data
 * `Execute`: committing, optionally fetches data

Running a simple query, accessing the returned data and serialising to JSON:
```python
>>> import pymssqlutils as sql
>>> result = sql.query(
      "SELECT SYSDATETIMEOFFSET() as now",
      server="..."
    )
>>> result.ok
True
>>> result.data
[{'now': datetime.datetime(2021, 1, 21, 23, 31, 11, 272299, tzinfo=datetime.timezone.utc)}]
>>> result.data[0]['now']
datetime.datetime(2021, 1, 21, 23, 31, 11, 272299, tzinfo=datetime.timezone.utc)
>>> result.to_json()
'[{"now":"2021-01-21T23:31:11.272299+00:00"}]'
```

Running a simple execution:

TODO

### Specifying Connection
There are two ways of specifying the connection parameters to the SQL Server:
1. Passing the required parameters
   ([see pymssql docs](https://pymssql.readthedocs.io/en/stable/ref/pymssql.html#pymssql.connect))
   to `query` or `execute` like in the quickstart example above.
   Note: All extra kwargs passed to these methods are passed on to the `pymssql.connection()`.
2. Specify the connection parameters in the environment like the example below. Note: that parameters given
   explicitly will take precedence over connection parameters specified in the environment.
   
```python
import os
import pymssqlutils as sql

os.environ["MSSQL_SERVER"] = "sqlserver.mycompany.com"
os.environ["MSSQL_USER"] = "my_login"
os.environ["MSSQL_PASSWORD"] = "my_password123"

result = sql.execute("INSERT INTO mytable VALUES (1, 'test)")
```

### Executing SQL
This library provides four functions for executing SQL code:
`query`, `execute`, `execute_many` & `execute_batched`.
These functions call _pymssql's_ `execute` or `executemany` functions with varying behaviour to fetching
result data or committing the transaction, see table below.

| Function         | Uses            |   commits     |  fetches     |
|------------------|-----------------|---------------|--------------|
| query            | execute         | False         |  True        |
| execute          | execute         | True          |  Optional    |
| execute_many     | executemany     | True          |  False       |
| execute_batched  | execute         | True          |  False       |

Splitting `query` & `execute` into two functions based on whether the execution
commits or not is intended to make your code clearer and more explicit.

### Error handling (TODO)

### Utility Functions (TODO)

## Testing (TODO)

Must install pytest to run main tests, that mock cursor results.
To test on_database tests against an MSSQL instance `"TEST_ON_DATABASE"` must be set in the environment
as well as any of the normal env variables to connect to the MSSQL server, `pytest-dotenv` can help with this.

## Notes

### Why _pymssql_ when Microsoft officially recommends _pyodbc_ (opinion)?

The main difference between _pyodbc_ and _pymssql_ is the drivers they use.
The ODBC are newer and have various levels of support on differing linux distributions,
and if you develop for containers or distribute code onto different platforms
you can run into ODBC driver-related issues that FreeTDS tends to not have.

There are other minor reasons someone might prefer _pymssql_, e.g.:
 * _pymssql's_ parameter subsitution is done client-side improving operation visibility.
 * _pymssql_ also has support for MSSQL specific data types such as `Datetimeoffset`.