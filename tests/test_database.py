import os
from datetime import datetime, timedelta, timezone

import pytest

import pymssqlutils as sql
from pymssqlutils import model_to_values
from tests.helpers import check_correct_types, generate_fake_data_query

SKIP_FILE = not os.environ.get("TEST_ON_DATABASE")
SKIP_REASON = "TEST_ON_DATABASE is not set in environment"


@pytest.mark.skipif(SKIP_FILE, reason=SKIP_REASON)
def test_can_connect():
    sql.query("SELECT sysdatetimeoffset() now")


@pytest.mark.skipif(SKIP_FILE, reason=SKIP_REASON)
def test_type_parsing_single_row():
    data = sql.query(generate_fake_data_query(1, 0)).data[0]

    check_correct_types(data)


@pytest.mark.skipif(SKIP_FILE, reason=SKIP_REASON)
def test_type_parsing_multi_row():
    rows = sql.query(generate_fake_data_query(50, 0)).data

    for data in rows:
        check_correct_types(data)


@pytest.mark.skipif(SKIP_FILE, reason=SKIP_REASON)
def test_type_parsing_large_multi_row():
    rows = sql.query(generate_fake_data_query(15000, 0)).data

    for data in rows:
        check_correct_types(data)

    assert len(rows) == 15000


@pytest.mark.skipif(SKIP_FILE, reason=SKIP_REASON)
def test_type_parsing_single_row_all_null():
    data = sql.query(generate_fake_data_query(1, 1)).data[0]

    for item in data.values():
        assert item is None


@pytest.mark.skipif(SKIP_FILE, reason=SKIP_REASON)
def test_type_parsing_multi_row_all_null():
    rows = sql.query(generate_fake_data_query(5, 1)).data

    for data in rows:
        for item in data.values():
            assert item is None


@pytest.mark.skipif(SKIP_FILE, reason=SKIP_REASON)
def test_type_parsing_multi_row_some_null():
    rows = sql.query(generate_fake_data_query(15, 0.5)).data

    for data in rows:
        check_correct_types(data, True)


@pytest.mark.skipif(SKIP_FILE, reason=SKIP_REASON)
def test_execute():
    sql.execute("CREATE TABLE #temp (val int)")


@pytest.mark.skipif(SKIP_FILE, reason=SKIP_REASON)
def test_execute_with_fetch():
    result = sql.execute("SELECT 'hello' col", fetch=True)
    assert result.data[0]["col"] == "hello"


@pytest.mark.skipif(SKIP_FILE, reason=SKIP_REASON)
def test_execute_many():
    sql.execute("SELECT %s val", [(1,), (2,), (3,)])


@pytest.mark.skipif(SKIP_FILE, reason=SKIP_REASON)
def test_execute_batched():
    sql.execute(
        "SELECT %s a, %s b, %s c",
        [(val, val + 1, val + 2) for val in range(1000)],
        batch_size=500,
    )


@pytest.mark.skipif(SKIP_FILE, reason=SKIP_REASON)
def test_execute_many_operations():
    result = sql.execute([f"SELECT {val} val" for val in range(100)], fetch=True)
    assert result.data[0]["val"] == 99


@pytest.mark.skipif(SKIP_FILE, reason=SKIP_REASON)
def test_datetime_offset_handling():
    model = {
        "col1": "hello",
        "col2": 1.23,
        "col3": datetime(2020, 6, 1, 12, 30, tzinfo=timezone(timedelta(hours=-1))),
        "col4": True,
    }
    result = sql.execute(
        [
            "CREATE TABLE #temp (col1 VARCHAR(100), col2 DECIMAL(6,2), col3 DATETIMEOFFSET, col4 TINYINT)",
            f"INSERT INTO #temp {model_to_values(model)}",
            "SELECT * FROM #temp",
        ],
        fetch=True,
    )
    assert result.data[0]["col1"] == "hello"
    assert result.data[0]["col2"] == 1.23
    assert result.data[0]["col3"] == datetime(
        2020, 6, 1, 12, 30, tzinfo=timezone(timedelta(hours=-1))
    )
    assert result.data[0]["col4"] == 1


@pytest.mark.skipif(SKIP_FILE, reason=SKIP_REASON)
def test_no_return():
    result = sql.query("SELECT TOP 0 'test' Col1")
    assert result.data == []
