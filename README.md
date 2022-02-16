# pymssql-utils
_pymssql-utils_ is a small library that wraps
[pymssql](https://github.com/pymssql/pymssql) to make your life easier.
It provides you with a higher-level API as well as various utility functions
for generating dynamic queries.

This module features:
* Higher-level API that reduces the amount of boilerplate required.
* Baked-in sensible defaults and usage patterns.
* Provides optional execution batching, similar to
  [_pyodbc's_](https://github.com/mkleehammer/pyodbc) `fast_executemany`.
* Provides consistent parsing between SQL Types and native Python types over different platforms and drivers.
* Makes it easy to serialize your data with
  [_orjson_](https://github.com/ijl/orjson).
* Provides you with simple and clear options for error handling.
* Extra utility functions for building dynamic SQL queries.
* Fixing various edge case bugs that arise when using _pymssql_.
* Fully type hinted.

This module's enforced opinions (check these work for you):
* Each execution opens and closes a connection using _pymssql_'s
  context management.
* Automatically converts certain data types for ease of use, e.g. `Decimal` -> `float`, `UUID` -> `str`.
  
When you shouldn't use this module:
* If you need fine-grained control over your cursors.
  
Please raise any suggestions or issues via GitHub.

## Status

This library is tested and stable. There will not be any breaking changes to the 
public API without a major version release.  There is also scope for expanding the 
library if new features are requested.

## Changes

See the repository's [GitHub releases](https://github.com/invokermain/pymssql-utils/releases)
or the `CHANGELOG.md`.

## Usage
### Installation

This library can be installed via pip: `pip install --upgrade pymssql-utils`.
This library requires `Python >= 3.7`.

If you want to serialize your results to JSON you can install the optional dependency `ORJSON`
by running `pip install --upgrade pymssql-utils[json]`.

If you want to cast your results to DataFrame you can install the optional dependency `Pandas`
by running `pip install --upgrade pymssql-utils[pandas]`.

### Quickstart

For querying the database this library provides two high-level methods:
 * `query`: executes a SQL operation that fetches the result, and DOES NOT commit the transaction (by default).
 * `execute`: executes a SQL operation that does not fetch the result (by default), and DOES commit the transaction.

This separation of _pymssql's_ `execute` is to make your code more explicit and readable.

Here is an example for running a simple query and accessing the returned data:
```python
>>> import pymssqlutils as sql
>>> result = sql.query(
      "SELECT SYSDATETIMEOFFSET() as now",
      server="..."
    )
>>> result.data
[{'now': datetime.datetime(2021, 1, 21, 23, 31, 11, 272299, tzinfo=datetime.timezone.utc)}]
```

And running a simple execution:
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
   All extra `**kwargs` passed to these methods are passed on to the `pymssql.connection()`.
2. Specify the connection parameters in the environment like the example below, this is the recommended way
   (however, any parameters given explicitly to a method will take priority).
   
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
 * `raise_errors (bool)`: whether to raise exceptions or to return the error information with the result.
 * Any extra kwargs are passed to _pymssql's_ `connect` method.

Returns a `DatabaseResult` class, see documentation below.

#### Execute

The `execute` method executes a SQL Operation which commits the transaction
& optionally returns the result (not by default).

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
   these can be a single value, tuple or dictionary OR this can be a list of any of the previous.
 * `batch_size (int)`: if specified concatenates the operations together according to the batch_size,
   this can vastly increase performance if executing many statements.
   Raises an error if set to True and both operations and parameters are singular.
 * `fetch (bool)`: if True returns the result from the LAST execution, by default false.  
 * `raise_errors (bool)`: whether to raise exceptions or to return the error information with the result.
 * Any extra kwargs are passed to _pymssql's_ `connect` method.

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

### DatabaseResult Class

One big difference between this library and _pymssql_ is that here
`execute` and `query` return an instance of the `DatabaseResult` class.

This class holds the returned data, if there is any, and provides
various useful attributes and methods to work with the result.

#### Attributes & Properties
 * `ok`: True if the execution did not error, else False. Only useful if using `raise_errors = False`,
   see below section on Error Handling.
 * `error`: Populated by the error raised during execution (if applicable). Only useful if using `raise_errors = False`.
 * `fetch`: True if results from the execution were fetched (e.g. if using `query`), else False.
 * `commit`: True if the execution was committed (i.e. if using `execute`), else False.
 * `columns`: A list of the column names in the dataset returned from the execution (if applicable)
 * `data`: The dataset returned from the execution (if applicable), this is a list of dictionaries.
 * `raw_data`: The dataset returned from the execution (if applicable), this is a list of tuples.
 * `set_count`: Returns the count of result sets that the execution returned, as an integer.

#### Methods
 * `to_dataframe`: (requires Pandas to be installed), returns the dataset as a DataFrame object.
   All args and kwargs are parsed to the DataFrame constructor.
 * `to_json`: returns the dataset as a json serialized string using the `orjson` library, make sure this 
   optional dependency is installed by running `pip install --upgrade pymssql-utils[json]`.
   Note that this will fail if your data contains `bytes` type values. By default, this method returns a string, but
   pass `as_bytes = True` to return a byte string. Specify `with_columns = True` to include the column names
   (rows as dictionaries instead of tuples).
 * `write_error_to_logger`: writes the error information to the library's logger, optionally pass a `name` parameter
   to allow you to easier indentify the query in the logging output.
 * `raise_error`: raises a `pymssqlutils.DatabaseError` from the underlying `pymssql` error,
   optionally pass a `name` parameter to allow you to easier indentify the query in the error output.
 * `next_set`: changes the class to return the data and metadata (columns etc) of the next result set. Returns True
   if there was a next set to move to, otherwise returns False and doesn't do anything.
 * `previous_set`: changes the class to return the data and metadata (columns etc) of the previous result set. Returns True
   if there was a previous set to move to, otherwise returns False and doesn't do anything.


### Error handling

Both `query` & `execute` take `raise_errors` as a parameter, which is by default `True`. This means that by default
_pymssql-utils_ will allow _pymssql_ to raise errors as normal.

Passing `raise_errors` as `False` will pass any errors onto the `DatabaseResult` class, which allows you
to handle errors gracefully using the `DatabaseResult` class (see above), e.g.:

```python
import pymssqlutils as db

result = db.query("Bad Operation", raise_errors=False)

if not result.ok: # result.ok will be False due to error
    
    # write the error to logging output
    result.write_error_to_logger('An optional query identifier to aid logging')
    
    # the error is stored under the error attribute
    error = result.error 
   
    # can always re-raise the error
    result.raise_error('Query Identifier')
```

This can be useful in situations where you do not want the error to propogate, e.g. if querying the database
as part of an API response.

### Utility Functions
#### set_connection_details

The `set_connection_details` method is a helper function which will set the value of
the relevant environment variable for the connection kwargs given.

Warning: this function has program wide side effects and will overwrite any
previously set connection details in the environment; therefore its usage is only recommended
in single script projects/notebooks. All the connections details will also be visible in your code.

The preferred method in production scenarios is to set the environment variables directly.

```python
def set_connection_details(
    server: str = None,
    database: str = None,
    user: str = None,
    password: str = None
) -> None:
```

Parameters:
* `server (str)`: the network address of the SQL server to connect to, sets 'MSSQL_SERVER' in the environment.
* `database (str)`: the default database to use on the SQL server, sets 'MSSQL_DATABASE' in the environment.
* `user (str)`: the user to authenticate against the SQL server with, sets 'MSSQL_USER' in the environment
* `password (str)`: the password to authenticate against the SQL server with, sets 'MSSQL_PASSWORD' in the environment

#### substitute_parameters

The `substitute_parameters` method does the same parameter substitution as `query` and `execute`, but returns the
substituted operation instead of executing it. This allows you to see the actual operation being run
against the database and is useful for debugging and logging.

```python
substitute_parameters(
    operation: str,
    parameters: SQLParameters
) -> str:
```

Parameters:
* `operation (str)`: The SQL operation requiring substitution.
* `parameters (SQLParameters)`: The parameters to substitute in.

Returns the parameter substituted SQL operation as a string.

Example:

```python3
>>> substitute_parameters("SELECT %s Col1, %s Col2", ("Hello", 1.23))
"SELECT N'Hello' Col1, 1.23 Col2"
```

#### to_sql_list

The `to_sql_list` method converts a Python iterable to a string form of the SQL equivalent list. This is useful
when creating dynamic SQL operations using the 'IN' operator.

```python
to_sql_list(
    listlike: Iterable[SQLParameter]
) -> str:
```

Parameters:
* `listlike (Iterable[SQLParameter])`: The iterable of SQLParameter to transform

Returns the SQL equivalent list as a string

Examples:

```python3
>>> to_sql_list([1, 'hello', datetime.now()])
"(1, N'hello', N'2021-03-22T10:56:27.981173')"
```

```python3
>>> my_ids = [1, 10, 21]
>>> f"SELECT * FROM MyTable WHERE Id IN {to_sql_list(my_ids)}"
'SELECT * FROM MyTable WHERE Id IN (1, 10, 21)'
```

#### model_to_values

The `model_to_values` method converts a Python mapping (e.g. dictionary of Pydantic model) to the SQL equivalent
values string. This is useful when creating dynamic SQL operations using the 'INSERT' statement.

```python3
model_to_values(
    model: Any,
    prepend: List[Tuple[str, str]] = None,
    append: List[Tuple[str, str]] = None,
) -> str:
```

Parameters:
* `model (Any)`: A mapping to transform, i.e. a dictionary or an object that has the __dict__ method implemented,
  with string keys and SQLParameter values.
* `prepend (List[Tuple[str, str]])`: prepend a variable number of columns to the beginning of the values statement.
* `append (List[Tuple[str, str]])`: append a variable number of columns to the end of the values statement.

Returns a string of the form: `([attr1], [attr2], ...) VALUES (val1, val2, ...)`.

Warning: prepended and appended columns are not parameter substituted,
this can leave your code open to SQL injection attacks.

Example:

```python3
>>> my_data = {'value': 1.56, 'insertDate': datetime.now()}

>>> model_to_values(my_data, prepend=[('ForeignId', '@Id')])
"([ForeignId], [value], [insertDate]) VALUES (@Id, 1.56, N'2021-03-22T13:58:33.758740')"

>>> f"INSERT IN MyTable {model_to_values(my_data, prepend=[('foreignId', '@Id')])}"
"INSERT IN MyTable ([foreignId], [value], [insertDate]) VALUES (@Id, 1.56, N'2021-03-22T13:58:33.758740')"
```

## Notes
### Type Parsing

_pymssql-utils_ parses SQL types to their native python types regardless of the environment.
This ensures consistent behaviour across various systems, see the table below for a comparison.

|                   | Windows         |            | Ubuntu          |            |
|-------------------|-----------------|------------|-----------------|------------|
| *SQL DataType*    | *pymssql-utils* | *pymssql*  | *pymssql-utils* | *pymssql*  |
| Date              | date            | date       | date            | str        |
| Binary            | bytes           | bytes      | bytes           | bytes      |
| Time1             | time            | time       | time            | str        |
| Time2             | time            | time       | time            | str        |
| Time3             | time            | time       | time            | str        |
| Time4             | time            | time       | time            | str        |
| Time5             | time            | time       | time            | str        |
| Time6             | time            | time       | time            | str        |
| Time7             | time            | time       | time            | str        |
| Small DateTime    | datetime        | datetime   | datetime        | datetime   |
| Datetime          | datetime        | datetime   | datetime        | datetime   |
| Datetime2         | datetime        | datetime   | datetime        | str        |
| DatetimeOffset0   | datetime        | bytes      | datetime        | str        |
| DatetimeOffset1   | datetime        | bytes      | datetime        | str        |
| DatetimeOffset2   | datetime        | bytes      | datetime        | str        |
| UniqueIdentifier  | str             | bytes      | str             | ???        |

## Testing

Install pytest to run non-integration tests via `pytest .`,
these tests mock the cursor results allowing the library to test locally.

To test against an MSSQL instance install `pytest-dotenv`.
Then create a `.env` file with `"TEST_ON_DATABASE"` set as a truthy value, as well as any
connection environemt variables for the MSSQL server.
These tests will then be run (not-skipped), e.g. `pytest . --envfile .test.env`

### Why _pymssql_ when Microsoft officially recommends _pyodbc_ (opinion)?

There are other minor reasons someone might prefer _pymssql_, e.g.:
1. _pymssql_ supports for MSSQL specific data types such as `Datetimeoffset`.
2. _pymssql's_ parameter subsitution is done client-side improving debug/logging ability.
3. _pymssql's_ drivers are easily installed, meaning that your code 'just works' in 
   more environments without extra steps, [e.g. this](https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver15)!