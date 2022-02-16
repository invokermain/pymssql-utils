import logging
import os
import warnings
from contextlib import contextmanager
from itertools import zip_longest
from typing import Any, Dict, Iterable, List, Tuple, Union

import pymssql as sql
from pymssql import Connection

from .databaseresult import DatabaseResult
from .helpers import SQLParameter, SQLParameters

logger = logging.getLogger(__name__)

TDS_PROTOCOL_CHECKED = False


def substitute_parameters(operation: str, parameters: SQLParameters) -> str:
    """
    This function returns the SQL operation that would be executed on the server (i.e.
    after parsing and substituting of parameters). Useful for logging & debugging.

    :param operation: The SQL operation requiring substitution
    :param parameters: The parameters to substitute in
    :return: The parameter substituted SQL operation as a string
    """
    if isinstance(parameters, tuple):
        parameters = tuple(
            item.isoformat() if hasattr(item, "isoformat") else item
            for item in parameters
        )
    elif isinstance(parameters, dict):
        parameters = {
            key: item.isoformat() if hasattr(item, "isoformat") else item
            for key, item in parameters.items()
        }
    else:
        if hasattr(parameters, "isoformat"):
            parameters = parameters.isoformat()
        elif parameters is None:
            parameters = (None,)

    return sql._mssql.substitute_params(operation, parameters).decode("UTF-8")


def set_connection_details(
    server: str = None, database: str = None, user: str = None, password: str = None
) -> None:
    """
    Sets the relevant environment variable for each passed parameter.

    :param server: the network address of the SQL server to connect to,
                   sets 'MSSQL_SERVER' in the environment
    :param database: the default database to use on the SQL server,
                     sets 'MSSQL_DATABASE' in the environment
    :param user: the user to authenticate against the SQL server with,
                 sets 'MSSQL_USER' in the environment
    :param password: the password to authenticate against the SQL server with,
                     sets 'MSSQL_PASSWORD' in the environment
    :return:
    """
    if server:
        os.environ["MSSQL_SERVER"] = server
    if database:
        os.environ["MSSQL_DATABASE"] = database
    if user:
        os.environ["MSSQL_USER"] = user
    if password:
        os.environ["MSSQL_PASSWORD"] = password


def _with_conn_details(kwargs: Dict) -> Dict:
    if not kwargs:
        kwargs = {}

    kwargs["server"] = kwargs.get("server", os.environ.get("MSSQL_SERVER"))
    kwargs["database"] = kwargs.get("database", os.environ.get("MSSQL_DATABASE"))
    kwargs["user"] = kwargs.get("user", os.environ.get("MSSQL_USER"))
    kwargs["password"] = kwargs.get("password", os.environ.get("MSSQL_PASSWORD"))

    if not kwargs["server"]:
        raise ValueError(
            "`server` must be passed as a parameter or `MSSQL_SERVER` "
            "must be set in the environment."
        )

    return kwargs


def query(
    operation: str,
    parameters: SQLParameters = None,
    raise_errors: bool = True,
    **kwargs,
) -> DatabaseResult:
    """
    Execute a SQL Operation which DOES NOT COMMIT the transaction & returns the result.

    **kwargs are passed through to the pymssql.connect() method.

    :param operation: the SQL Operation to execute
    :type operation: str
    :param parameters: parameters to substitute into the operation.
    :type parameters: SQLParameters
    :param raise_errors: if True raises errors, else DatabaseResult class will
                         contain the error details
    :type raise_errors: bool, optional
    :return: a DatabaseResult class.
    :rtype: DatabaseResult
    """
    try:
        return _execute(
            [operation],
            [parameters] if parameters else None,
            fetch=True,
            **_with_conn_details(kwargs),
        )
    except sql.Error as err:
        if raise_errors:
            raise err
        return DatabaseResult(ok=False, fetch=True, commit=False, error=err)


def execute(
    operations: Union[str, List[str]],
    parameters: Union[SQLParameters, List[SQLParameters]] = None,
    batch_size: int = None,
    fetch: bool = False,
    raise_errors: bool = True,
    **kwargs,
) -> DatabaseResult:
    """
    Used for a SQL Operation/s which COMMIT the transaction.
    There are two ways of using this function:

    Passing in a single operation (str) to operations:
        * If parameters is singular, this calls pymssql.execute()
          and executes a single operation
        * If parameters is plural, this calls pymssql.execute_many()
          and executes one execution per parameter set

    Passing in multiple operations (List[str]) to operations:
        * If parameters is None, this calls pymssql.execute_many()
          and executes one execution per operation
        * If parameters is the same length as operations, this calls pymssql.execute()
          multiple times and executes one execution per operation.

    Optionally batch_size can be specified to use string concatenation to batch
    the operations, this can provide significant performance gains if executing 100+
    small operations. This is similar to fast_executemany found in pyodbc package. A
    value of 500-1000 is a good default.

    Fetch can be set to True, this will only return the last result set.

    :param operations: the SQL Operation/s to execute. If this is a list then
                       parameters needs to be None or a list of the same length.
    :type operations: Union[str, List[str]]
    :param parameters: parameters to substitute into the operation. These can be a
                       single value, tuple or dictionary. If operations is a list
                       this parameter needs to either be None or a list of the
                       same length.
    :type parameters: Union[SQLParameters, List[SQLParameters]], optional
    :param batch_size: If specified concatenate the operations together
                       according to the batch_size, raises an error if set to True
                       and both operations and parameters are singular
    :type batch_size: int, optional
    :param fetch: return the LAST result of the execution
    :type fetch: bool, optional
    :param raise_errors: if True raises errors, else DatabaseResult class will
                         contain the error details
    :type raise_errors: bool, optional
    :return: a DatabaseResult class
    :rtype: DatabaseResult
    """
    if isinstance(operations, str):
        operations = [operations]
    if parameters is not None and not isinstance(parameters, list):
        parameters = [parameters]

    singular_operations = len(operations) == 1
    singular_parameters = parameters and len(parameters) == 1

    # validate
    if batch_size is not None:
        if singular_operations and singular_parameters:
            raise ValueError(
                "batch_size cannot be used if both operations "
                "and parameters are singular"
            )
        if batch_size <= 0:
            raise ValueError("batch_size cannot be negative")

    if parameters is not None:
        if not (singular_parameters or singular_operations) and len(operations) != len(
            parameters
        ):
            raise ValueError(
                "parameters must be the same length as "
                "operations if they are both lists"
            )

    try:
        if batch_size:
            return _execute_batched(
                operations, parameters, batch_size, fetch, **_with_conn_details(kwargs)
            )
        return _execute(
            operations,
            parameters,
            commit=True,
            fetch=fetch,
            **_with_conn_details(kwargs),
        )
    except sql.Error as err:
        if raise_errors:
            raise err
        return DatabaseResult(
            ok=False,
            fetch=fetch,
            commit=True,
            error=err,
        )


@contextmanager
def _get_connection(**kwargs) -> Connection:
    conn = sql.connect(**kwargs)
    global TDS_PROTOCOL_CHECKED
    if not TDS_PROTOCOL_CHECKED:
        tds_major, tds_minor = conn._conn.tds_version_tuple
        if tds_major < 7 or tds_minor < 3:
            message = (
                f"Your connection is trying to use TDS Protocol {tds_major}.{tds_minor}"
                ", this protocol does not support time, date, "
                "datetime2 or datetimeoffset. "
                "It is strongly suggested you upgrade pymssql to the latest version."
            )
            warnings.warn(message, RuntimeWarning)
        TDS_PROTOCOL_CHECKED = True
    yield conn


def _execute(
    operations: List[str],
    parameters: List[SQLParameters] = None,
    commit: bool = False,
    fetch: bool = False,
    **kwargs,
) -> DatabaseResult:
    """
    This is an internal method, you should call execute() instead
    """
    with _get_connection(**kwargs) as cnxn:
        with cnxn.cursor() as cur:
            if parameters:
                fillvalue = (
                    parameters[-1]
                    if len(parameters) < len(operations)
                    else operations[-1]
                )
                for operation, parameter_set in zip_longest(
                    operations, parameters, fillvalue=fillvalue
                ):
                    cur.execute(substitute_parameters(operation, parameter_set))
            else:
                for operation in operations:
                    cur.execute(operation)

            result = DatabaseResult(ok=True, fetch=fetch, commit=commit, cursor=cur)

        if commit:
            cnxn.commit()

    return result


def _execute_batched(
    operations: List[str],
    parameters: List[SQLParameters] = None,
    batch_size: int = 1000,
    fetch: bool = False,
    **kwargs,
) -> DatabaseResult:
    """
    This is an internal method, you should call execute() instead
    """
    if parameters:
        fillvalue = (
            parameters[-1] if len(parameters) < len(operations) else operations[-1]
        )
        batched = [
            "\n;".join(
                substitute_parameters(operation, parameter_set)
                for operation, parameter_set in zip_longest(
                    operations[i : i + batch_size],
                    parameters[i : i + batch_size],
                    fillvalue=fillvalue,
                )
            )
            for i in range(0, max(len(parameters), len(operations)), batch_size)
        ]
    else:
        batched = [
            "\n;".join(operation for operation in operations[i : i + batch_size])
            for i in range(0, len(operations), batch_size)
        ]

    with _get_connection(**_with_conn_details(kwargs)) as cnxn:
        with cnxn.cursor() as cur:
            for batch in batched:
                cur.execute(batch)
            result = DatabaseResult(ok=True, fetch=fetch, commit=True, cursor=cur)
        cnxn.commit()
    return result


def to_sql_list(listlike: Iterable[SQLParameter]) -> str:
    """
    Transforms an iterable to a SQL list string.
    Intended to be used with the SQL 'in' operator when building dynamic SQL queries.

    e.g. [1, 2, 3] -> '(1, 2, 3)'

    :param listlike: The iterable of SQLParameter to transform
    :return: str
    """
    out_str = ", ".join(substitute_parameters("%s", x) for x in listlike)
    return f"({out_str})"


def model_to_values(
    model: Any,
    prepend: List[Tuple[str, str]] = None,
    append: List[Tuple[str, str]] = None,
) -> str:
    """
    Transforms a Dict or Mapping into a string of the form:
    '([attr1], [attr2], ...) VALUES (val1, val2, ...)'.
    Intended to be used when creating dynamic SQL INSERT statements.

    Prepend and append can be used to add a SQL column with a static or variable value
    at the beginning or end of the values list.

    e.g. passing prepend = [('prependedColumn', '@prependedColumn')] would return a
    string of the form:
    '([prependedColumn], [attr1], ...) VALUES (@prependedColumn, val1, ...)'

    Note: prepended and appended columns are not parameter substituted,
          this can leave your code open to sql injection.

    :param model: a Dictionary or anything that implements the __dict__ method
                  (e.g. a Pydantic Model)
    :param prepend: prepend a variable number of columns to the beginning of the
                    values statement.
    :param append: append a variable number of columns to the end of the
                   values statement.
    :return: str
    """
    keys = [x[0] for x in prepend] if prepend else []
    prepend_values = [x[1] for x in prepend] if prepend else []
    append_values = [x[1] for x in append] if append else []

    if isinstance(model, dict):
        keys.extend(model.keys())
        values = list(model.values())
    else:
        keys.extend(model.__dict__.keys())
        values = list(model.__dict__.values())

    if append:
        keys.extend(x[0] for x in append)

    values = ", ".join(
        [
            *map(str, prepend_values),
            # keep (x,) due to: https://github.com/pymssql/pymssql/issues/696
            *[substitute_parameters("%s", (x,)) for x in values],
            *map(str, append_values),
        ]
    )

    column_names = "(" + ", ".join(f"[{key}]" for key in keys) + ")"
    return f"{column_names} VALUES ({values})"
