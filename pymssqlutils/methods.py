import datetime as dt
import logging
import os
from decimal import Decimal
from typing import (
    Dict,
    Optional,
    Any,
    List,
    Tuple,
    Union,
    Iterable,
    Mapping,
)

import pymssql as sql

from .databaseresult import DatabaseResult
from .helpers import SQLParameters

logger = logging.getLogger(__name__)


def with_conn_details(kwargs: Dict) -> Dict:
    if not kwargs:
        kwargs = {}
    try:
        kwargs["database"] = kwargs.get("database") or os.environ.get(
            "DB_NAME", "MASTER"
        )
        kwargs["server"] = kwargs.get("server") or os.environ["DB_SERVER"]
        kwargs["user"] = kwargs.get("user") or os.environ["DB_USER"]
        kwargs["password"] = kwargs.get("password") or os.environ["DB_PASSWORD"]
        return kwargs
    except KeyError:
        raise EnvironmentError(
            "Please specify the connection details as parameters or in the environment."
        )


def execute(
    operation: str,
    parameters: SQLParameters = None,
    fetch: bool = False,
    raise_errors: bool = True,
    **kwargs,
) -> DatabaseResult:
    """
    Wrapper of pymssql's cursor.execute() which COMMITS the transaction, and optionally returns the result set.

    :param operation: str, the SQL Operation to execute.
    :param parameters: parameters to substitute into the operation. These can be a single value, tuple or dictionary.
    :param fetch: bool, optionally return the result of the execution. Default is False.
    :param raise_errors: if True raises errors, else DatabaseResult class will contain the error details.
    :return: a DatabaseResult class.
    """
    try:
        return _execute(
            operation,
            parameters,
            commit=True,
            many=False,
            fetch=fetch,
            **with_conn_details(kwargs),
        )
    except sql.Error as err:
        if raise_errors:
            raise err
        return DatabaseResult(
            ok=False,
            execution_args=("commit", "fetch") if fetch else ("commit",),
            error=err,
        )


def execute_many(
    operation: str, parameters: List[SQLParameters], raise_errors: bool = True, **kwargs
) -> DatabaseResult:
    """
    Wrapper of pymssql's cursor.executemany() which COMMITS the transactions, and does not return the results.

    **kwargs are passed through to the pymssql.connect() method.

    :param operation: str, the SQL Operation to execute.
    :param parameters: parameters to substitute into the operations.
                       Expects a list of single values, tuples or dictionaries.
    :param raise_errors: if True raises errors, else DatabaseResult class will contain the error details.
    :return: a DatabaseResult class.
    """
    try:
        return _execute(
            operation,
            parameters,
            commit=True,
            many=True,
            **with_conn_details(kwargs),
        )
    except sql.Error as err:
        if raise_errors:
            raise err
        return DatabaseResult(ok=False, execution_args=("commit", "many"), error=err)


def query(
    operation: str,
    parameters: SQLParameters = None,
    raise_errors: bool = True,
    **kwargs,
) -> DatabaseResult:
    """
    A shorthand for a call to execute that fetches the result and DOES NOT commit the transaction.
    **kwargs are passed through to the pymssql.connect() method.

    :param operation: str, the SQL Operation to execute.
    :param parameters: parameters to substitute into the operation. These can be a single value, tuple or dictionary.
    :param raise_errors: if True raises errors, else DatabaseResult class will contain the error details.
    :return: a DatabaseResult class.
    """
    try:
        return _execute(
            operation,
            parameters,
            fetch=True,
            **with_conn_details(kwargs),
        )
    except sql.Error as err:
        if raise_errors:
            raise err
        return DatabaseResult(ok=False, execution_args=("fetch",), error=err)


def _execute(
    operation: str,
    parameters: Union[Tuple[Any, ...], List[Tuple[Any, ...]]] = None,
    commit: bool = False,
    fetch: bool = False,
    many: bool = False,
    **kwargs,
) -> DatabaseResult:
    """
    Internal method that does the work of executing the query and handling the result.

    :param conn_details: The database connection details
    :param command: The name of the command being executed
    :param operation: The query string
    :param parameters: Parameters to pass to the cursor execute method
    :param commit: Whether to commit the transaction, default False
    :param fetch: Whether to fetch the results, default False
    :param many: Whether to use executemany instead of execute, default False
    :return: DatabaseResult class
    """
    commands = [
        command
        for command, arg in [("commit", commit), ("fetch", fetch), ("many", many)]
        if arg
    ]
    with sql.connect(as_dict=True, **kwargs) as cnxn:
        with cnxn.cursor() as cur:
            if many:
                cur.executemany(operation, parameters)
            else:
                cur.execute(operation, parameters)

            data = cur.fetchall() if fetch else None

        if commit:
            cnxn.commit()

    return DatabaseResult(data=data, ok=True, execution_args=tuple(commands))


def substitute_parameters(operation: str, parameters: SQLParameters) -> str:
    """
    This function returns the SQL code that would be executed on the server (i.e. after
    parsing and substituting the parameters.

    :param operation: The query string
    :param parameters: The parameters to substitute
    :return: the parameter substituted sql string
    """
    return sql._mssql.substitute_params(operation, parameters).decode("UTF-8")


def to_sql_list(listlike: Iterable) -> str:
    """
    Transforms an iterable to a SQL list string.
    Intended to be used with the SQL 'in' operator when building dynamic SQL queries.

    e.g. [1, 2, 3] -> '(1, 2, 3)'

    :param listlike: The iterable of objects to transform
    :return: str
    """
    out_str = ", ".join(substitute_parameters("%s", x) for x in listlike)
    return f"({out_str})"


def model_to_values(
    model: Mapping,
    prepend: Optional[Tuple[str, str]] = None,
    append: Optional[Tuple[str, str]] = None,
) -> str:
    """
    Transforms a Dict or Mapping into a string of the form: '(attr1, attr2, ...) VALUES (val1, val2, ...)'.
    Intended to be used when creating dynamic SQL INSERT statements.

    Prepend and append can be used to add a SQL column with a static or variable value
    at the beginning or end of the values list.

    e.g. passing prepend = [('prependedColumn', '@prependedColumn')] would return a string of the form:
    '(prependedColumn, attr1, attr2, ...) VALUES (@prependedColumn, val1, val2, ...)'

    :param model: a Dictionary or anything that implements the __dict__ method (e.g. a Pydantic Model)
    :param prepend: prepend a variable number of columns to the beginning of the values statement.
    :param append: append a variable number of columns to the end of the values statement.
    :return: str
    """

    def clean(item):
        if isinstance(item, Decimal):
            return float(item)
        if isinstance(item, dt.datetime):
            return item.isoformat()
        else:
            return item

    if prepend:
        keys = [x[0] for x in prepend]
        values = [x[1] for x in prepend]
    else:
        keys = []
        values = []

    if isinstance(model, dict):
        keys.extend(model.keys())
        values.extend(model.values())
    else:
        keys.extend(model.__dict__.keys())
        values.extend(model.__dict__.values())

    if append:
        keys.extend(x[0] for x in append)
        values.extend(x[1] for x in append)

    column_names = "(" + ", ".join(keys) + ")"
    properties = tuple(clean(v) for v in values)

    property_placeholders = ["%s"] * len(properties)
    properties = substitute_parameters(
        "(" + ", ".join(property_placeholders) + ")",
        properties,
    )
    return f"{column_names} VALUES {properties}"
