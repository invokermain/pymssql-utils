import sys

import pandas
import pymssql
import pytest

from pymssqlutils import DatabaseResult
from pymssqlutils.databaseresult import _cursor_generator
from tests.helpers import (
    MockCursor,
    check_correct_types,
    cursor_description,
    cursor_row,
)


def test_no_data_errors():
    result = DatabaseResult(
        ok=True,
        fetch=False,
        commit=False,
    )

    with pytest.raises(ValueError):
        result.data
    with pytest.raises(ValueError):
        result.raw_data
    with pytest.raises(ValueError):
        result.columns
    with pytest.raises(ValueError):
        result.source_types


def test_no_cursor_but_fetch():
    with pytest.raises(ValueError):
        DatabaseResult(ok=True, fetch=True, commit=False)


def test_write_error_but_ok():
    result = DatabaseResult(ok=True, fetch=False, commit=False)
    with pytest.raises(ValueError, match=".*did not error"):
        result.write_error_to_logger()


def test_write_error_no_error():
    result = DatabaseResult(ok=False, fetch=False, commit=False)
    with pytest.raises(ValueError, match=".*error message"):
        result.write_error_to_logger()


def test_raise_error_but_ok():
    result = DatabaseResult(ok=True, fetch=False, commit=False)
    with pytest.raises(ValueError, match=".*did not error"):
        result.raise_error()


def test_raise_error_but_no_error():
    result = DatabaseResult(ok=False, fetch=False, commit=False)
    with pytest.raises(ValueError, match=".*error instance"):
        result.raise_error()


def test_write_error(caplog):
    result = DatabaseResult(
        ok=False, fetch=False, commit=False, error=pymssql.Error("123", "abc")
    )
    result.write_error_to_logger()
    assert "Error" in caplog.text


def test_data_parsing():
    result = DatabaseResult(
        ok=True, fetch=True, commit=False, cursor=MockCursor(row_count=1)
    )
    check_correct_types(result.data[0])


def test_source_types():
    result = DatabaseResult(
        ok=True, fetch=True, commit=False, cursor=MockCursor(row_count=1)
    )
    assert result.source_types
    assert isinstance(result.source_types, tuple)
    assert isinstance(result.source_types[0], int)


def test_unhandled_data_parsing():
    description = (("Col_Range", 3, None, None, None, None, None),)
    cursor = MockCursor(row_count=1, description=description, row=[(range(1, 2),)])
    with pytest.warns(RuntimeWarning):
        result = DatabaseResult(ok=True, fetch=True, commit=False, cursor=cursor)

    assert result.data[0]["Col_Range"] == range(1, 2)


def test_correct_row_count_large_query():
    result = DatabaseResult(
        ok=True, fetch=True, commit=False, cursor=MockCursor(row_count=25000)
    )
    assert len(result.raw_data) == 25000
    assert len(result.data) == 25000


def test_cursor_generator():
    count = 0
    row_count = 0
    rows = 25000
    for batch in _cursor_generator(MockCursor(row_count=rows)):
        row_count += len(batch)
        count += 1

    assert count == (rows // 10000) + 1
    assert row_count == rows


def test_to_json():
    description = list(cursor_description)
    example_row = list(cursor_row[0])

    # remove bytes as not JSON Serializable
    for idx in (37, 9, 8):
        example_row.pop(idx)
        description.pop(idx)

    result = DatabaseResult(
        ok=True,
        fetch=True,
        commit=False,
        cursor=MockCursor(
            row_count=3, row=[tuple(example_row)], description=tuple(description)
        ),
    )

    assert isinstance(result.to_json(), str)
    assert isinstance(result.to_json(as_bytes=True), bytes)


def test_to_json_no_orjson(monkeypatch):
    description = list(cursor_description)
    example_row = list(cursor_row[0])

    monkeypatch.setitem(sys.modules, "orjson", None)

    result = DatabaseResult(
        ok=True,
        fetch=True,
        commit=False,
        cursor=MockCursor(
            row_count=3, row=[tuple(example_row)], description=tuple(description)
        ),
    )

    with pytest.raises(ImportError):
        result.to_json()


def test_to_json_no_data():
    result = DatabaseResult(
        ok=True,
        fetch=False,
        commit=False,
    )

    with pytest.raises(ValueError):
        result.to_json()


def test_cast_to_dataframe():
    result = DatabaseResult(
        ok=True, fetch=True, commit=False, cursor=MockCursor(row_count=5)
    )
    df = result.to_dataframe()

    assert isinstance(df, pandas.DataFrame)
    assert df.columns.tolist() == list(result.columns)


def test_cast_to_dataframe_no_pandas(monkeypatch):
    monkeypatch.setitem(sys.modules, "pandas", None)

    result = DatabaseResult(
        ok=True, fetch=True, commit=False, cursor=MockCursor(row_count=5)
    )

    with pytest.raises(ImportError):
        result.to_dataframe()
