import logging
import struct
import uuid
import warnings
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    NoReturn,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

import pymssql as sql
from pymssql import Cursor, InterfaceError, OperationalError
from pymssql._mssql import MSSQLDatabaseException, MSSQLDriverException

from pymssqlutils.helpers import SQLParameter

if TYPE_CHECKING:
    from pandas import DataFrame

logger = logging.getLogger(__name__)
T = TypeVar("T")
_result_set = Tuple[List[Tuple[Any, ...]], Tuple[str, ...], Tuple[int, ...]]


def _parse_datetimeoffset_from_bytes(item: bytes) -> datetime:
    # fix for certain versions of FreeTDS driver returning Datetimeoffset as bytes
    microseconds, days, tz, _ = struct.unpack("QIhH", item)
    return datetime(
        1900, 1, 1, 0, 0, 0, tzinfo=timezone(timedelta(minutes=tz))
    ) + timedelta(days=days, minutes=tz, microseconds=microseconds / 10)


def _identity(x: T) -> T:
    return x


def _unset(x: T) -> T:
    return x


def _get_data_mapper(sql_type_hint: int, item: Any) -> Callable[[Any], SQLParameter]:
    if sql_type_hint == 1:  # STRING: str
        if isinstance(item, str):
            return _identity

    if sql_type_hint == 2:  # BINARY: bytes, datetime, time, date
        if isinstance(item, (datetime, date, time)):
            return _identity
        if isinstance(item, uuid.UUID):
            return str
        if isinstance(item, bytes):
            try:
                _parse_datetimeoffset_from_bytes(item)
                return _parse_datetimeoffset_from_bytes
            except (ValueError, struct.error):
                # it's not a datetime
                return _identity

    if sql_type_hint == 3:  # NUMBER: int, float
        if isinstance(item, (int, float)):
            return _identity

    if sql_type_hint == 4:  # DATETIME: datetime
        if isinstance(item, datetime):
            return _identity

    if sql_type_hint == 5:  # DECIMAL: float (forced), int
        # warning: bigint is returned as Decimal type
        if isinstance(item, Decimal):
            return float

    warnings.warn(
        f"unhandled type ({sql_type_hint}, {item}, {type(item)})", RuntimeWarning
    )
    return _identity


def _get_cleaned_data(
    cursor: Cursor, source_types: Tuple[int, ...]
) -> List[Tuple[Any, ...]]:
    data_mappers: List[Callable[[Any], Any]] = [_unset] * len(source_types)

    def clean_item(idx: int, item: Any) -> Any:
        if item is None:
            return None

        data_mapper = data_mappers[idx]

        if data_mapper is _unset:
            data_mapper = _get_data_mapper(source_types[idx], item)
            data_mappers[idx] = data_mapper
            return data_mapper(item)

        return data_mapper(item)

    try:
        return [
            tuple(clean_item(e, item) for e, item in enumerate(row)) for row in cursor
        ]
    except MSSQLDatabaseException as err:
        raise OperationalError(err.args[0])
    except MSSQLDriverException as err:
        raise InterfaceError(err.args[0])


def _get_result_set(cursor: Cursor) -> _result_set:
    columns = tuple(x[0] for x in cursor.description)
    source_types = tuple(x[1] for x in cursor.description)
    data = _get_cleaned_data(cursor, source_types)
    return data, columns, source_types


def _get_result_sets(cursor: Cursor) -> Tuple[_result_set]:
    if cursor.description is None:
        return tuple()

    result_sets = [_get_result_set(cursor)]
    while cursor.nextset():
        result_sets.append(_get_result_set(cursor))

    return tuple(result_sets)


class DatabaseError(Exception):
    pass


class DatabaseResult:
    ok: bool
    fetch: bool
    commit: bool
    error: Optional[sql.Error]
    _columns: Optional[Tuple[str, ...]]
    _source_types: Optional[Tuple[int, ...]]
    _data: Optional[List[Tuple[SQLParameter, ...]]]
    _result_sets: Optional[Tuple[_result_set]]
    _current_result_set_index: int

    def __init__(
        self,
        ok: bool,
        fetch: bool,
        commit: bool,
        cursor: sql.Cursor = None,
        error: sql.Error = None,
    ):
        """
        This should not be initialised directly, instead it will be returned when
        calling the `execute` or `query` methods within the model.
        """
        self.ok = ok
        self.fetch = fetch
        self.commit = commit
        self.error = error
        self._columns = None
        self._source_types = None
        self._data = None
        self._result_sets = None
        self._current_result_set_index = 0

        if self.error:
            return

        if fetch:
            if cursor is None:
                raise ValueError("cursor must be passed to fetch data")

            self._result_sets = _get_result_sets(cursor)

            if self._result_sets:
                self._set_result_set()

    @property
    def source_types(self) -> Tuple[int, ...]:
        """
        Returns the current result set's source types as a Tuple of integers

        Raises a ValueError if there are no columns to return.
        """
        if self._source_types is not None:
            return self._source_types
        self._raise_no_data_error()

    @property
    def columns(self) -> Tuple[str, ...]:
        """
        Returns the current result set's columns as a Tuple of strings

        Raises a ValueError if there are no columns to return.
        """
        if self._columns is not None:
            return self._columns
        self._raise_no_data_error()

    @property
    def data(self) -> List[Dict[str, Any]]:
        """
        Returns the current result set's data as a List of Dictionaries with column
        names as the key.

        Raises a ValueError if there is no data to return.
        """
        if self._data is not None:
            return [
                {self.columns[e]: item for e, item in enumerate(row)}
                for row in self._data
            ]
        self._raise_no_data_error()

    @property
    def raw_data(self) -> List[Tuple[Any, ...]]:
        """
        Returns the current result set's data as a List of Tuples.

        Raises a ValueError if there is no data to return.
        """
        if self._data is not None:
            return self._data
        self._raise_no_data_error()

    @property
    def set_count(self) -> int:
        """
        Returns the count of current result sets returned by the execution.

        Raises a ValueError if there are no result sets.
        """
        if self._result_sets is not None:
            return len(self._result_sets)
        self._raise_no_data_error()

    def next_set(self) -> bool:
        """
        Sets the DatabaseResult class to use the next result set that
        the execution returned.

        Returns False if there are no more sets in this direction, otherwise True.
        """
        if self._result_sets is not None:
            if self._current_result_set_index == len(self._result_sets) - 1:
                return False
            self._current_result_set_index += 1
            self._set_result_set()
            return True
        self._raise_no_data_error()

    def previous_set(self) -> bool:
        """
        Sets the DatabaseResult class to use the previous result set that
        the execution returned.

        Returns False if there are no more sets in this direction, otherwise True.
        """
        if self._result_sets is not None:
            if self._current_result_set_index == 0:
                return False
            self._current_result_set_index -= 1
            self._set_result_set()
            return True
        self._raise_no_data_error()

    def write_error_to_logger(self, name: str = "unknown") -> None:
        """
        Writes the error to logger.

        :param name: str, an optional name to show in the error string.
        """
        if self.ok:
            raise ValueError("This execution did not error")
        if self.error is None:
            raise ValueError("This execution did not provide an error message")
        error_text = str(
            self.error.args[1] if len(self.error.args) >= 2 else self.error
        )
        logger.error(
            f"DatabaseResult Error (<{name}|fetch={self.fetch},commit={self.commit}>)"
            f": <{type(self.error).__name__}> {error_text}"
        )

    def raise_error(self, name: str = "unknown") -> NoReturn:
        """
        Raises a pymssql DatabaseError with an optional name to help identify
        the operation.

        :param name: str, an optional name to show in the error string.
        """
        if self.ok:
            raise ValueError("This execution did not error")
        if self.error is None:
            raise ValueError("This execution did not provide an error instance")
        raise DatabaseError(
            f"<{name}|fetch={self.fetch},commit={self.commit}> bad execution"
        ) from self.error

    def to_dataframe(self, *args, **kwargs) -> "DataFrame":
        """
        Return the data as a Pandas DataFrame, all args and kwargs are passed to
        the DataFrame initiation method.

        :return: a DataFrame
        """
        # noinspection PyUnresolvedReferences
        try:
            from pandas import DataFrame
        except ImportError:
            raise ImportError(
                "Pandas must be installed to use this method"
            ) from ImportError

        # if there is no data, but we know the columns, return an empty dataframe
        if not self._data and self._columns:
            return DataFrame(columns=self._columns)

        return DataFrame(data=self.data, *args, **kwargs)

    def to_json(
        self, as_bytes: bool = False, with_columns: bool = False
    ) -> Union[bytes, str]:
        """
        Returns the serialized data as a JSON format string.
        :params as_bytes: bool, if True returns the JSON object as UTF-8 encoded bytes
                          instead of string
        :params with_columns: bool, if True serializes the data property, otherwise
                              serializes the raw_data property.
        :return: Union[bytes, str]
        """
        # noinspection PyUnresolvedReferences
        try:
            from orjson import dumps
        except ImportError as err:
            raise ImportError("ORJSON must be installed to use this method") from err

        data_ = self.data if with_columns else self.raw_data
        json_ = dumps(data_)

        if as_bytes:
            return json_

        return json_.decode("UTF-8")

    def _raise_no_data_error(self) -> NoReturn:
        if not self.fetch:
            raise ValueError(
                "This DatabaseResult was initialised with fetch=False, "
                "and therefore has no data."
            )
        if not self.ok:
            raise ValueError(
                "This DatabaseResult was not successful, and therefore has no data."
            )
        raise ValueError("This DatabaseResult returned no data.")

    def _set_result_set(self) -> None:
        """
        Decomposes the current result set and assigns the values to the relevant
        attributes
        """
        if self._result_sets is not None:
            self._columns = self._result_sets[self._current_result_set_index][1]
            self._source_types = self._result_sets[self._current_result_set_index][2]
            self._data = self._result_sets[self._current_result_set_index][0]
        else:
            self._raise_no_data_error()
