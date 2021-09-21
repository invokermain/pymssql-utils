from datetime import datetime, timedelta, timezone

import pymssql
import pytest
from pytest_mock import MockerFixture

import pymssqlutils as sql
import pymssqlutils.methods
from pymssqlutils import (
    DatabaseResult,
    model_to_values,
    set_connection_details,
    substitute_parameters,
    to_sql_list,
)
from pymssqlutils.databaseresult import cursor_generator
from pymssqlutils.methods import _with_conn_details
from tests.helpers import (
    MockCursor,
    check_correct_types,
    cursor_description,
    cursor_row,
)


def test_with_conn_details_from_args():
    conn_details = _with_conn_details(
        {
            "database": "database",
            "server": "server",
            "user": "user",
            "password": "password",
        }
    )
    assert conn_details["database"] == "database"
    assert conn_details["server"] == "server"
    assert conn_details["user"] == "user"
    assert conn_details["password"] == "password"


def test_with_conn_details_from_env(monkeypatch):
    monkeypatch.setenv("MSSQL_DATABASE", "database")
    monkeypatch.setenv("MSSQL_SERVER", "server")
    monkeypatch.setenv("MSSQL_USER", "user")
    monkeypatch.setenv("MSSQL_PASSWORD", "password")
    conn_details = _with_conn_details({})
    assert conn_details["database"] == "database"
    assert conn_details["server"] == "server"
    assert conn_details["user"] == "user"
    assert conn_details["password"] == "password"


def test_set_connection_details(monkeypatch):
    set_connection_details(
        database="database", server="server", user="user", password="password"
    )
    conn_details = _with_conn_details({})
    assert conn_details["database"] == "database"
    assert conn_details["server"] == "server"
    assert conn_details["user"] == "user"
    assert conn_details["password"] == "password"


def test_execute_single_operations_no_params(mocker: MockerFixture, monkeypatch):
    monkeypatch.setenv("MSSQL_SERVER", "server")
    conn = mocker.patch("pymssqlutils.methods._get_connection", autospec=True)
    cursor = (
        conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
    )
    result = sql.execute("test query")
    assert cursor.execute.call_args_list == [(("test query",),)]
    assert isinstance(result, DatabaseResult)
    assert result.data is None
    assert result.ok
    assert result.commit


def test_execute_single_operations_single_params(mocker: MockerFixture, monkeypatch):
    monkeypatch.setenv("MSSQL_SERVER", "server")
    conn = mocker.patch("pymssqlutils.methods._get_connection", autospec=True)
    cursor = (
        conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
    )
    result = sql.execute("select %s val", 2)
    assert cursor.execute.call_args_list == [
        (("select 2 val",),),
    ]
    assert isinstance(result, DatabaseResult)
    assert result.data is None
    assert result.ok
    assert result.commit


def test_execute_single_operations_multiple_params(mocker: MockerFixture, monkeypatch):
    monkeypatch.setenv("MSSQL_SERVER", "server")
    conn = mocker.patch("pymssqlutils.methods._get_connection", autospec=True)
    cursor = (
        conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
    )
    result = sql.execute("select %s val", [2, 3, 4])
    assert cursor.execute.call_args_list == [
        (("select 2 val",),),
        (("select 3 val",),),
        (("select 4 val",),),
    ]
    assert isinstance(result, DatabaseResult)
    assert result.data is None
    assert result.ok
    assert result.commit


def test_execute_single_operations_multiple_params_batched(
    mocker: MockerFixture, monkeypatch
):
    monkeypatch.setenv("MSSQL_SERVER", "server")
    conn = mocker.patch("pymssqlutils.methods._get_connection", autospec=True)
    cursor = (
        conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
    )
    result = sql.execute("select %s val", [2, 3, 4, 5], batch_size=2)
    assert cursor.execute.call_args_list == [
        (("select 2 val\n;select 3 val",),),
        (("select 4 val\n;select 5 val",),),
    ]
    assert isinstance(result, DatabaseResult)
    assert result.data is None
    assert result.ok
    assert result.commit


def test_execute_multiple_operations_no_params(mocker: MockerFixture, monkeypatch):
    monkeypatch.setenv("MSSQL_SERVER", "server")
    conn = mocker.patch("pymssqlutils.methods._get_connection", autospec=True)
    cursor = (
        conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
    )
    result = sql.execute(["test query", "second query"])
    assert cursor.execute.call_args_list == [
        (("test query",),),
        (("second query",),),
    ]
    assert isinstance(result, DatabaseResult)
    assert result.data is None
    assert result.ok
    assert result.commit


def test_execute_multiple_operations_single_param(mocker: MockerFixture, monkeypatch):
    monkeypatch.setenv("MSSQL_SERVER", "server")
    conn = mocker.patch("pymssqlutils.methods._get_connection", autospec=True)
    cursor = (
        conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
    )
    result = sql.execute(["select %s val", "select %s val"], 1)
    assert cursor.execute.call_args_list == [
        (("select 1 val",),),
        (("select 1 val",),),
    ]
    assert isinstance(result, DatabaseResult)
    assert result.data is None
    assert result.ok
    assert result.commit


def test_execute_multiple_operations_single_param_batched(
    mocker: MockerFixture, monkeypatch
):
    monkeypatch.setenv("MSSQL_SERVER", "server")
    conn = mocker.patch("pymssqlutils.methods._get_connection", autospec=True)
    cursor = (
        conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
    )
    result = sql.execute(["select %s val"] * 4, 1, batch_size=2)
    assert cursor.execute.call_args_list == [
        (("select 1 val\n;select 1 val",),),
        (("select 1 val\n;select 1 val",),),
    ]
    assert isinstance(result, DatabaseResult)
    assert result.data is None
    assert result.ok
    assert result.commit


def test_execute_multiple_operations_multiple_params(
    mocker: MockerFixture, monkeypatch
):
    monkeypatch.setenv("MSSQL_SERVER", "server")
    conn = mocker.patch("pymssqlutils.methods._get_connection", autospec=True)
    cursor = (
        conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
    )
    result = sql.execute(["select %s val", "select %s val"], [1, 2])
    assert cursor.execute.call_args_list == [
        (("select 1 val",),),
        (("select 2 val",),),
    ]
    assert isinstance(result, DatabaseResult)
    assert result.data is None
    assert result.ok
    assert result.commit


def test_execute_multiple_operations_multiple_params_batched(
    mocker: MockerFixture, monkeypatch
):
    monkeypatch.setenv("MSSQL_SERVER", "server")
    conn = mocker.patch("pymssqlutils.methods._get_connection", autospec=True)
    cursor = (
        conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
    )
    result = sql.execute(
        ["select %s val1", "select %s val2", "select %s val3", "select %s val4"],
        [1, 2, 3, 4],
        batch_size=2,
    )
    assert cursor.execute.call_args_list == [
        (("select 1 val1\n;select 2 val2",),),
        (("select 3 val3\n;select 4 val4",),),
    ]
    assert isinstance(result, DatabaseResult)
    assert result.data is None
    assert result.ok
    assert result.commit


def test_query(mocker: MockerFixture, monkeypatch):
    monkeypatch.setenv("MSSQL_SERVER", "server")
    mocker.patch("pymssqlutils.databaseresult.cursor_generator", return_value=[])
    conn = mocker.patch("pymssqlutils.methods._get_connection", autospec=True)
    cursor = (
        conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
    )
    result = sql.query("test query")
    assert cursor.execute.call_args_list == [
        (("test query",),),
    ]
    assert isinstance(result, DatabaseResult)
    assert result.data is not None
    assert result.ok
    assert result.fetch
    assert not result.commit


def test_data_parsing():
    result = DatabaseResult(
        ok=True, fetch=True, commit=False, cursor=MockCursor(row_count=1)
    )
    check_correct_types(result.data[0])


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
    for batch in cursor_generator(MockCursor(row_count=rows)):
        row_count += len(batch)
        count += 1

    assert count == (rows // 10000) + 1
    assert row_count == rows


def test_data_serializable():
    pytest.importorskip("orjson")

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


def test_result_error_handling_on_ignore(mocker):
    mocker.patch("pymssqlutils.methods._execute", side_effect=pymssql.OperationalError)
    result = sql.query(
        "test query",
        raise_errors=False,
    )
    assert result.ok is False
    assert isinstance(result.error, pymssql.OperationalError)
    assert result.data is None


def test_result_error_handling_on_ignore_and_raise(mocker):
    mocker.patch("pymssqlutils.methods._execute", side_effect=pymssql.OperationalError)
    result = sql.query("test query", raise_errors=False)
    with pytest.raises(sql.DatabaseError):
        result.raise_error()


def test_result_error_handling_on_raise(mocker):
    mocker.patch("pymssqlutils.methods._execute", side_effect=pymssql.OperationalError)
    with pytest.raises(pymssql.OperationalError):
        sql.query(
            "test query",
        )


def test_cast_to_dataframe():
    pandas = pytest.importorskip("pandas")
    result = DatabaseResult(
        ok=True, fetch=True, commit=False, cursor=MockCursor(row_count=5)
    )
    df = result.to_dataframe()

    assert isinstance(df, pandas.DataFrame)
    assert df.columns.tolist() == list(result.columns)


def test_dict_model_to_values():
    model = {
        "text": "hello",
        "float": 1.23,
        "datetime": datetime(2020, 6, 1, 12, 30, tzinfo=timezone(timedelta(hours=-1))),
        "bool": True,
        "none": None,
    }
    expected = (
        "([text], [float], [datetime], [bool], [none]) VALUES "
        "(N'hello', 1.23, N'2020-06-01T12:30:00-01:00', 1, NULL)"
    )
    assert model_to_values(model) == expected


def test_dict_model_to_values_with_prepend():
    model = {
        "text": "hello",
        "float": 1.23,
    }
    assert (
        model_to_values(
            model, prepend=[("pre1", "@var1"), ("pre2", "SYSDATETIMEOFFSET()")]
        )
        == "([pre1], [pre2], [text], [float]) VALUES (@var1, SYSDATETIMEOFFSET(), N'hello', 1.23)"
    )


def test_dict_model_to_values_with_append():
    model = {
        "text": "hello",
        "float": 1.23,
    }
    assert (
        model_to_values(
            model, append=[("pre1", "@var1"), ("pre2", "SYSDATETIMEOFFSET()")]
        )
        == "([text], [float], [pre1], [pre2]) VALUES (N'hello', 1.23, @var1, SYSDATETIMEOFFSET())"
    )


def test_substitute_parameters_tuple():
    params = (
        1.23,
        datetime(2020, 6, 1, 12, 30, tzinfo=timezone(timedelta(hours=-1))),
        "hello",
        True,
        None,
    )
    query = "%s, %s, %s, %s, %s"
    assert (
        substitute_parameters(query, params)
        == "1.23, N'2020-06-01T12:30:00-01:00', N'hello', 1, NULL"
    )


def test_substitute_parameters_single():
    params = (
        1.23,
        datetime(2020, 6, 1, 12, 30, tzinfo=timezone(timedelta(hours=-1))),
        "hello",
        True,
    )
    expected = ("1.23", "N'2020-06-01T12:30:00-01:00'", "N'hello'", "1")
    query = "%s"
    for param, expected in zip(params, expected):
        assert substitute_parameters(query, param) == expected


def test_to_sql_list():
    assert (
        to_sql_list(
            (
                1.23,
                datetime(2020, 6, 1, 12, 30, tzinfo=timezone(timedelta(hours=-1))),
                "hello",
                True,
                None,
            )
        )
        == "(1.23, N'2020-06-01T12:30:00-01:00', N'hello', 1, NULL)"
    )
