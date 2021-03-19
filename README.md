# pymssql-utils (BETA)
_pymssql-utils_ is a small library that wraps
[pymssql](https://github.com/pymssql/pymssql) to make your life easier.
It provides a higher-level API so that you can think less about connections and cursors,
and more about SQL.

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
* If performance is an absolute must (use [_pyodbc_](https://github.com/mkleehammer/pyodbc))
  
Please raise any suggestions or issues via GitHub.

## Status

This library is in beta, meaning that pending any major issues
I do not expect to make any breaking changes to the public API.
However, there might still be a few bugs to be found. There is scope for expanding the library
if new features are requested.

## Usage
### Installation

This library can be installed via pip: `pip install --upgrade pymssql-utils`.
This library requires `Python >= 3.6` and `Pip >= 19.3`.

### Quickstart

This library provides two high-level methods:
 * `query`: fetches the result, DOES NOT commit the transaction.
 * `execute`: which by default does not fetch the result, and DOES commit the transaction.

This separation of _pymssql's_ `execute` is to make your code more explicit, concise and discoverable, allowed as
the library is not _DB_API_ compliant anyway.

An example for running a simple query, accessing the returned data and serialising to JSON:
```python
>>> import pymssqlutils as sql
>>> result = sql.query(
      "SELECT SYSDATETIMEOFFSET() as now",
      server="..."
    )
>>> result.data
[{'now': datetime.datetime(2021, 1, 21, 23, 31, 11, 272299, tzinfo=datetime.timezone.utc)}]
>>> result.data[0]['now']
datetime.datetime(2021, 1, 21, 23, 31, 11, 272299, tzinfo=datetime.timezone.utc)
>>> result.to_json()
'[{"now":"2021-01-21T23:31:11.272299+00:00"}]'
```

Running a simple execution:

```python
>>> import pymssqlutils as sql
>>> result = sql.execute(
      "INSERT INTO mytable VALUES (1, 'test')",
      server="MySQLServer"
    )
```

### Specifying Connection
There are two ways of specifying the connection parameters to the SQL Server:
1. Passing the required parameters
   ([see pymssql docs](https://pymssql.readthedocs.io/en/stable/ref/pymssql.html#pymssql.connect))
   to `query` or `execute` like in the quickstart example above.
   Note: All extra kwargs passed to these methods are passed on to the `pymssql.connection()`.
2. Specify the connection parameters in the environment like the example below, this is the recommended way.
   Note: any parameters given explicitly will take precedence over connection parameters specified in the environment.
   
```python
import os
import pymssqlutils as sql

os.environ["MSSQL_SERVER"] = "sqlserver.mycompany.com"
os.environ["MSSQL_USER"] = "my_login"
os.environ["MSSQL_PASSWORD"] = "my_password123"

result = sql.execute("INSERT INTO mytable VALUES (%s, %s)", (1, "test"))
```

There is a helper method to set this in code, see `set_connection_details` below.

### Executing SQL
#### Query

The `query` method executes a SQL Operation which does not commit the transaction & returns the result.

```python
query(
    operation: str,
    parameters: SQLParameters = None,
    raise_errors: bool = True,
    **kwargs,
) -> DatabaseResult:
```

Parameters:
 * `operation (str)`: the SQL operation to execute.
 * `parameters (SQLParameters)`: parameters to substitute into the operation,
   these can be a single value, tuple or dictionary.
 * `raise_errors (bool)`: whether to raise exceptions or to ignore them
   and let you handle the error yourself via the DatabaseResult class.
 * Any kwargs are passed to _pymssql's_ `connect` method.

Returns a `DatabaseResult` class, see documentation below.

#### Execute

The `execute` method executes a SQL Operation which commits the transaction
& optionally returns the result (by default False).

```python
execute(
    operations: Union[str, List[str]],
    parameters: Union[SQLParameters, List[SQLParameters]] = None,
    batch_size: int = None,
    fetch: bool = False,
    raise_errors: bool = True,
    **kwargs,
) -> DatabaseResult:
```

Parameters:
 * `operations (Union[str, List[str]])`: the SQL Operation/s to execute.
 * `parameters (Union[SQLParameters, List[SQLParameters]])`: parameters to substitute into the operation/s,
   these can be a single value, tuple or dictionary OR this can be a list of these.
 * `batch_size (int)`: if specified concatenates the operations together according to the batch_size,
   this can vastly increase performance if executing many statements.
   Raises an error if set to True and both operations and parameters are singular
 * `fetch (bool)`: if True returns the result from the LAST execution, default False.  
 * `raise_errors (bool)`: whether to raise exceptions or to ignore them
   and let you handle the error yourself via the DatabaseResult class.
 * Any kwargs are passed to _pymssql's_ `connect` method.

Returns a `DatabaseResult` class, see documentation below.

There are two ways of using this function:

Passing in a single operation (`str`) to operations:
* If parameters is singular, this calls `pymssql.execute()` and executes a single operation
* If parameters is plural, this calls `pymssql.execute_many()` and executes one execution per parameter set

Passing in multiple operations (`List[str]`) to operations:
* If parameters is None, this calls `pymssql.execute_many()` and executes one execution per operation
* If parameters is the same length as operations, this calls `pymssql.execute()` multiple times
  and executes one execution per operation.

Optionally `batch_size` can be specified to use string concatenation to batch the operations, this can
provide significant performance gains if executing 100+ small operations. This is similar to `fast_executemany`
found in the `pyodbc` package. A value of 500-1000 is a good default.

### Error handling

For both `query` & `execute` take a `raise_errors` parameter which is by default `True`. This means that errors will be
raised to the stack as expected.

Specifying `raise_errors` to be `False` will pass any errors onto the `DatabaseResult` class which allows the developer
to handle errors themselves, for example:

```python
import pymssqlutils as db

result = db.query("Bad Operation", raise_errors=False)

if not result.ok: # result.ok will be False due to error
    
    # write the error to logging output
    result.write_error_to_logger('An optional query identifier to aid logging')
    
    # the error is stored under the error attribute
   error = result.error 
   
    # can always reraise the error
    result.raise_error('Query Identifier')
```

### Utility Functions (TODO)

## Notes
### Type Parsing

_pymssql-utils_ parses SQL types to their native python types regardless of the environment.
This ensures consistent behaviour across various systems, see the table below for a comparison.

|                 | Windows       |          | Ubuntu        |          |
|-----------------|---------------|----------|---------------|----------|
| SQL DataType    | pymssql-utils | pymssql  | pymssql-utils | pymssql  |
| Date            | date          | date     | date          | str      |
| Binary          | bytes         | bytes    | bytes         | bytes    |
| Time1           | time          | time     | time          | str      |
| Time2           | time          | time     | time          | str      |
| Time3           | time          | time     | time          | str      |
| Time4           | time          | time     | time          | str      |
| Time5           | time          | time     | time          | str      |
| Time6           | time          | time     | time          | str      |
| Time7           | time          | time     | time          | str      |
| Small DateTime  | datetime      | datetime | datetime      | datetime |
| Datetime        | datetime      | datetime | datetime      | datetime |
| Datetime2       | datetime      | datetime | datetime      | str      |
| DatetimeOffset0 | datetime      | bytes    | datetime      | str      |
| DatetimeOffset1 | datetime      | bytes    | datetime      | str      |
| DatetimeOffset2 | datetime      | bytes    | datetime      | str      |

## Testing

Must install pytest to run main tests, that mock cursor results.
To test on_database tests against an MSSQL instance `"TEST_ON_DATABASE"` must be set in the environment
as well as any of the normal env variables to connect to the MSSQL server, `pytest-dotenv` can help with this.

### Why _pymssql_ when Microsoft officially recommends _pyodbc_ (opinion)?

The main difference between _pyodbc_ and _pymssql_ is the drivers they use.
The ODBC drivers are newer and have various levels of support on differing linux distributions,
and if you develop for containers or distribute code onto different platforms
you can run into ODBC driver-related issues that FreeTDS tends to not have.

There are other minor reasons someone might prefer _pymssql_, e.g.:
 * _pymssql's_ parameter subsitution is done client-side improving operation visibility.
 * _pymssql_ also has built in support for MSSQL specific data types such as `Datetimeoffset`.