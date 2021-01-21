from datetime import date, time, datetime, timedelta, timezone
import logging
import re
import struct
from decimal import Decimal
from typing import Dict, Any, List, Union, Optional

import pymssql as sql
from dateutil.parser import isoparse
from orjson import dumps

logger = logging.getLogger(__name__)

date_regex = re.compile(r"(\d{4})-(\d{2})-(\d{2})")
time_regex = re.compile(r"(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,6})\d?)?")
datetime2_regex = re.compile(r"\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}.\d{7}")
datetimeoffset_regex = re.compile(
    r"(\d{4}-\d{2}-\d{2})\s(\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?)\d?\s([+-]\d{2}:\d{2})"
)


def _is_time(x):
    return re.fullmatch(time_regex, x)


def _is_date(x):
    return re.fullmatch(date_regex, x)


def _is_datetimeoffset(x):
    return re.fullmatch(datetimeoffset_regex, x)


def _is_datetime2(x):
    return re.fullmatch(datetime2_regex, x)


def _clean(item: Any) -> Any:
    if isinstance(item, str):
        # handle various date/time returns that pymssql doesn't handle
        match = _is_date(item)
        if match:
            return date(int(match[1]), int(match[2]), int(match[3]))

        match = _is_time(item)
        if match:
            return time(
                int(match[1]),
                int(match[2]),
                int(match[3]),
                microsecond=int(match[4]) if match[4] else 0,
            )

        match = _is_datetime2(item)
        if match:
            return isoparse(item[:-1])

        match = _is_datetimeoffset(item)
        if match:
            return isoparse(f"{match[1]}T{match[2]}" + (match[3] or ""))

    if isinstance(item, bytes):
        # fix for certain versions of FreeTDS driver returning Datetimeoffset as bytes
        microseconds, days, tz, _ = struct.unpack("QIhH", item)
        return datetime(
            1900, 1, 1, 0, 0, 0, tzinfo=timezone(timedelta(minutes=tz))
        ) + timedelta(days=days, minutes=tz, microseconds=microseconds / 10)

    if isinstance(item, Decimal):
        return float(item)

    return item


class DatabaseError:
    name: str
    message: str

    def __init__(self, name: str, message: Union[str, bytes]):
        self.name = name

        if isinstance(message, bytes):
            message = message.decode("utf-8")

        self.message = message


class DatabaseResult:
    ok: bool
    command: str
    columns: List[str] = None
    data: List[Dict[str, Optional[Union[str, int, float, time, date, datetime]]]] = None
    error: DatabaseError = None

    def __init__(
        self,
        ok: bool,
        command: str,
        data: List[Dict] = None,
        error: DatabaseError = None,
    ):
        self.ok = ok
        self.command = command
        self.error = error

        if data:
            self.columns = [*data[0]]
            self.data = [{k: _clean(v) for k, v in row.items()} for row in data]

    def write_error_to_logger(self, name: str) -> None:
        logger.error(
            f"DatabaseResult Error on {self.command} ({name})"
            f": <{self.error.name}> {self.error.message}"
        )

    def raise_error(self, name: str = None) -> None:
        """
        Raises a pymssql DatabaseError with an optional name to help identify the operation.
        :param name: str, an optional name to show in the error string.
        :return: None
        """
        name = f" ({name})" if name else ""
        raise sql.DatabaseError(
            f"{self.command}{name}: <{self.error.name}> {self.error.message}"
        )

    def to_dataframe(self, *args, **kwargs):
        """
        Return the data as a Pandas DataFrame, all args and kwargs are passed to
        the DataFrame initiation method.
        :return: a DataFrame
        """
        if not self.data:
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
        if as_bytes:
            return dumps(self.data)
        else:
            return dumps(self.data).decode("UTF-8")
