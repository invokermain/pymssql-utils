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
from pymssql import Cursor

from pymssqlutils.helpers import SQLParameter

if TYPE_CHECKING:
    from pandas import DataFrame

logger = logging.getLogger(__name__)
T = TypeVar("T")


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


def _cursor_generator(cursor_: Cursor, size: int = 10000):
    while True:
        out = cursor_.fetchmany(size=size)
        if not out:
            return
        yield out


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

    def clean_batch(items: Tuple[Tuple[Any, ...]]) -> Tuple[Tuple[Any, ...]]:
        return tuple(
            tuple(clean_item(e, item) for e, item in enumerate(row)) for row in items
        )

    return [item for items in _cursor_generator(cursor) for item in clean_batch(items)]


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

    def __init__(
        self,
        ok: bool,
        fetch: bool,
        commit: bool,
        cursor: sql.Cursor = None,
        error: sql.Error = None,
    ):
        self.ok = ok
        self.fetch = fetch
        self.commit = commit
        self.error = error
        self._columns = None
        self._source_types = None
        self._data = None

        if self.error:
            return

        if fetch:
            if cursor is None:
                raise ValueError("cursor must be passed to fetch")

            self._columns = tuple(x[0] for x in cursor.description)
            self._source_types = tuple(x[1] for x in cursor.description)
            self._data = _get_cleaned_data(cursor, self._source_types)

    @property
    def source_types(self) -> Tuple[int, ...]:
        if self._source_types is not None:
            return self._source_types
        self._raise_no_data_error()

    @property
    def columns(self) -> Tuple[str, ...]:
        if self._columns is not None:
            return self._columns
        self._raise_no_data_error()

    @property
    def data(self) -> List[Dict[str, Any]]:
        if self._data is not None:
            return [
                {self.columns[e]: item for e, item in enumerate(row)}
                for row in self._data
            ]
        self._raise_no_data_error()

    @property
    def raw_data(self) -> List[Tuple[Any, ...]]:
        if self._data is not None:
            return self._data
        self._raise_no_data_error()

    def write_error_to_logger(self, name: str = "unknown") -> None:
        """
        Writes the error to logger.

        :param name: str, an optional name to show in the error string.
        :return: None
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

    def raise_error(self, name: str = "unknown") -> None:
        """
        Raises a pymssql DatabaseError with an optional name to help identify
        the operation.

        :param name: str, an optional name to show in the error string.
        :return: None
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
        if self.data is None:
            raise ValueError("DatabaseResult class has no data to cast to DataFrame")

        # noinspection PyUnresolvedReferences
        try:
            from pandas import DataFrame
        except ImportError:
            raise ImportError(
                "Pandas must be installed to use this method"
            ) from ImportError

        return DataFrame(data=self.data, *args, **kwargs)

    def to_json(self, as_bytes=False) -> Union[bytes, str]:
        """
        returns the serialized data as a JSON format string.
        :params as_bytes: bool, if True returns the JSON object as UTF-8 encoded bytes
                          instead of string
        :return: Union[bytes, str]
        """

        if not self._data:
            raise ValueError("DatabaseResult class has no data to cast to DataFrame")

        # noinspection PyUnresolvedReferences
        try:
            from orjson import dumps
        except ImportError as err:
            raise ImportError(
                "ORJSON must be installed to use this method, you can install "
                + "this by running `pip install --upgrade pymssql-utils[json]`"
            ) from err

        if as_bytes:
            return dumps(self._data)
        else:
            return dumps(self._data).decode("UTF-8")

    def _raise_no_data_error(self) -> NoReturn:
        if not self.fetch:
            raise ValueError(
                "This DatabaseResult was initialised with fetch=False, "
                "and therefore has no data."
            )
        if not self.ok:
            raise ValueError(
                "This DatabaseResult was not successful, " "and therefore has no data."
            )
        raise ValueError("This DatabaseResult has no data.")
