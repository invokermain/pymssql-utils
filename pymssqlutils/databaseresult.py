import logging
import struct
import uuid
import warnings
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import pymssql as sql

from pymssqlutils.helpers import SQLParameter

logger = logging.getLogger(__name__)


def _parse_datetimeoffset_from_bytes(item: bytes) -> datetime:
    # fix for certain versions of FreeTDS driver returning Datetimeoffset as bytes
    microseconds, days, tz, _ = struct.unpack("QIhH", item)
    return datetime(
        1900, 1, 1, 0, 0, 0, tzinfo=timezone(timedelta(minutes=tz))
    ) + timedelta(days=days, minutes=tz, microseconds=microseconds / 10)


def identity(x):
    return x


def cursor_generator(cursor_, size=10000):
    while True:
        out = cursor_.fetchmany(size=size)
        if not out:
            return
        yield out


def _get_data_mapper(sql_type_hint: int, item: Any) -> Callable[[Any], SQLParameter]:
    if sql_type_hint == 1:  # STRING: str
        if isinstance(item, str):
            return identity

    if sql_type_hint == 2:  # BINARY: bytes, datetime, time, date
        if isinstance(item, (datetime, date, time)):
            return identity
        if isinstance(item, uuid.UUID):
            return str
        if isinstance(item, bytes):
            try:
                _parse_datetimeoffset_from_bytes(item)
                return _parse_datetimeoffset_from_bytes
            except (ValueError, struct.error):
                # it's not a datetime
                return identity

    if sql_type_hint == 3:  # NUMBER: int, float
        if isinstance(item, (int, float)):
            return identity

    if sql_type_hint == 4:  # DATETIME: datetime
        if isinstance(item, datetime):
            return identity

    if sql_type_hint == 5:  # DECIMAL: float (forced), int
        # warning: bigint is returned as Decimal type
        if isinstance(item, Decimal):
            return float

    warnings.warn(
        f"unhandled type ({sql_type_hint}, {item}, {type(item)})", RuntimeWarning
    )
    return identity


class DatabaseError(Exception):
    pass


class DatabaseResult:
    ok: bool
    fetch: bool
    commit: bool
    columns: Optional[Tuple[str, ...]]
    error: sql.Error
    source_types: Optional[Tuple[int, ...]]
    _data_mappers: Optional[List[Optional[Callable[[Any], SQLParameter]]]]
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
        self.columns = None
        self.source_types = None
        self._data_mappers = None
        self._data = None

        if self.error:
            return

        if fetch:
            self.columns = tuple(x[0] for x in cursor.description)
            self.source_types = tuple(x[1] for x in cursor.description)
            self._data_mappers = [None] * len(self.source_types)
            self._data = [
                item
                for items in cursor_generator(cursor)
                for item in self._clean_batch(items)
            ]

    def _clean_batch(self, items) -> Tuple[Tuple[SQLParameter, ...]]:
        return tuple(
            tuple(self._clean_item(e, item) for e, item in enumerate(row))
            for row in items
        )

    def _clean_item(self, idx: int, item: Any) -> SQLParameter:
        if item is None:
            return None

        if self._data_mappers[idx] is not None:
            return self._data_mappers[idx](item)

        data_mapper = _get_data_mapper(self.source_types[idx], item)

        if data_mapper is None:
            return item

        self._data_mappers[idx] = data_mapper
        return self._data_mappers[idx](item)

    @property
    def data(self) -> Optional[List[Dict[str, SQLParameter]]]:
        if self._data is not None:
            return [
                {self.columns[e]: item for e, item in enumerate(row)}
                for row in self._data
            ]
        return None

    @property
    def raw_data(self) -> Optional[List[Tuple[SQLParameter, ...]]]:
        if self._data is not None:
            return self._data
        return None

    def write_error_to_logger(self, name: str = "unknown") -> None:
        """
        Writes the error to logger.

        :param name: str, an optional name to show in the error string.
        :return: None
        """
        if self.ok:
            raise ValueError("This execution did not error")
        error_text = str(
            self.error.args[1] if len(self.error.args) >= 2 else self.error
        )
        logger.error(
            f"DatabaseResult Error (<{name}|fetch={self.fetch},commit={self.commit}>)"
            f": <{type(self.error).__name__}> {error_text}"
        )

    def raise_error(self, name: str = "unknown") -> None:
        """
        Raises a pymssql DatabaseError with an optional name to help identify the operation.
        :param name: str, an optional name to show in the error string.
        :return: None
        """
        if self.ok:
            raise ValueError("This execution did not error")
        raise DatabaseError(
            f"<{name}|fetch={self.fetch},commit={self.commit}> bad execution"
        ) from self.error

    def to_dataframe(self, *args, **kwargs):
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
            raise RuntimeError("Pandas must be installed to use this method")

        return DataFrame(data=self.data, *args, **kwargs)

    def to_json(self, as_bytes=False) -> Union[bytes, str]:
        """
        returns the serialized data as a JSON format string.
        :params as_bytes: bool, if True returns the JSON object as UTF-8 encoded bytes instead of string
        :return: Union[bytes, str]
        """

        if not self._data:
            raise ValueError("DatabaseResult class has no data to cast to DataFrame")

        # noinspection PyUnresolvedReferences
        try:
            from orjson import dumps
        except ImportError:
            raise RuntimeError(
                "ORJSON must be installed to use this method, you can install "
                + "this by running `pip install --upgrade pymssql-utils[json]`"
            )

        if as_bytes:
            return dumps(self.data)
        else:
            return dumps(self.data).decode("UTF-8")
