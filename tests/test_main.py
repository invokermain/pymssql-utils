from datetime import date, time, datetime, timezone, timedelta

import pymssql
import pytest
from pandas import DataFrame
from pytest_mock import MockerFixture

import pymssqlutils as sql
from pymssqlutils import DatabaseResult
from pymssqlutils.methods import (
    with_conn_details,
    model_to_values,
    substitute_parameters,
    to_sql_list,
)
from tests.helpers import MockCursor


def test_with_conn_details_from_args():
    conn_details = with_conn_details(
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
    conn_details = with_conn_details({})
    assert conn_details["database"] == "database"
    assert conn_details["server"] == "server"
    assert conn_details["user"] == "user"
    assert conn_details["password"] == "password"


def test_execute_single_operations_no_params(mocker: MockerFixture, monkeypatch):
    monkeypatch.setenv("MSSQL_SERVER", "server")
    conn = mocker.patch.object(pymssql, "connect", autospec=True)
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
    conn = mocker.patch.object(pymssql, "connect", autospec=True)
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
    conn = mocker.patch.object(pymssql, "connect", autospec=True)
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
    conn = mocker.patch.object(pymssql, "connect", autospec=True)
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
    conn = mocker.patch.object(pymssql, "connect", autospec=True)
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
    conn = mocker.patch.object(pymssql, "connect", autospec=True)
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
    conn = mocker.patch.object(pymssql, "connect", autospec=True)
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
    conn = mocker.patch.object(pymssql, "connect", autospec=True)
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
    conn = mocker.patch.object(pymssql, "connect", autospec=True)
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
    conn = mocker.patch.object(pymssql, "connect", autospec=True)
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


def test_data_parsing(monkeypatch):
    result = DatabaseResult(
        ok=True, fetch=True, commit=False, data=MockCursor().fetchall(), error=None
    )
    data = result.data[0]

    assert isinstance(data["Col_Date"], date)
    assert isinstance(data["Col_Time1"], time)
    assert isinstance(data["Col_Time2"], time)
    assert isinstance(data["Col_Time3"], time)
    assert isinstance(data["Col_Time4"], time)
    assert isinstance(data["Col_Time5"], time)
    assert isinstance(data["Col_Time6"], time)
    assert isinstance(data["Col_Time7"], time)
    assert isinstance(data["Col_Smalldatetime"], datetime)
    assert isinstance(data["Col_Datetime"], datetime)
    assert isinstance(data["Col_Datetime2"], datetime)
    assert isinstance(data["Col_Datetimeoffset0"], datetime)
    assert isinstance(data["Col_Datetimeoffset1"], datetime)
    assert isinstance(data["Col_Datetimeoffset2"], datetime)
    assert isinstance(data["Col_Datetimeoffset3"], datetime)
    assert isinstance(data["Col_Datetimeoffset4"], datetime)
    assert isinstance(data["Col_Datetimeoffset5"], datetime)
    assert isinstance(data["Col_Datetimeoffset6"], datetime)
    assert isinstance(data["Col_Datetimeoffset7"], datetime)
    assert isinstance(data["Col_Numeric"], float)


def test_data_serializable():
    result = DatabaseResult(
        ok=True, fetch=True, commit=False, data=MockCursor().fetchall(), error=None
    )

    assert isinstance(result.to_json(), str)
    assert isinstance(result.to_json(as_bytes=True), bytes)


def test_result_error_handling_on_ignore():
    # this will throw a pymssql.OperationalError
    result = sql.query(
        "test query",
        user="junk",
        server="doesnotexist",
        password="bad",
        raise_errors=False,
    )
    assert result.ok is False
    assert isinstance(result.error, pymssql.OperationalError)
    assert result.data is None


def test_result_error_handling_on_ignore_and_raise(monkeypatch):
    # this will throw a pymssql.OperationalError
    result = sql.query(
        "test query",
        user="junk",
        server="doesnotexist",
        password="bad",
        raise_errors=False,
    )
    with pytest.raises(sql.DatabaseError):
        result.raise_error()


def test_result_error_handling_on_raise(monkeypatch):
    # this will throw a pymssql.OperationalError
    with pytest.raises(pymssql.OperationalError):
        sql.query(
            "test query",
            user="junk",
            server="doesnotexist",
            password="bad",
        )


def test_cast_to_dataframe():
    pandas = pytest.importorskip("pandas")
    result = DatabaseResult(
        ok=True, fetch=True, commit=False, data=MockCursor().fetchall(), error=None
    )
    df = result.to_dataframe()

    assert isinstance(df, DataFrame)
    assert df.columns.tolist() == result.columns


def test_dict_model_to_values():
    model = {
        "text": "hello",
        "float": 1.23,
        "datetime": datetime(2020, 6, 1, 12, 30, tzinfo=timezone(timedelta(hours=-1))),
        "bool": True,
        "none": None,
    }
    assert (
        model_to_values(model)
        == "(text, float, datetime, bool, none) VALUES (N'hello', 1.23, N'2020-06-01T12:30:00-01:00', 1, NULL)"
    )


def test_dict_model_to_values_with_prepend():
    model = {
        "text": "hello",
        "float": 1.23,
    }
    assert (
        model_to_values(model, prepend=[("pre1", "0.123"), ("pre2", "@test")])
        == "(pre1, pre2, text, float) VALUES (N'0.123', N'@test', N'hello', 1.23)"
    )


def test_dict_model_to_values_with_append():
    model = {
        "text": "hello",
        "float": 1.23,
    }
    assert (
        model_to_values(model, append=[("pre1", "0.123"), ("pre2", "@test")])
        == "(text, float, pre1, pre2) VALUES (N'hello', 1.23, N'0.123', N'@test')"
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


def test_substitute_parameters_single_none_raises_error():
    with pytest.raises(ValueError):
        substitute_parameters("%s", None)


def test_listlike_to_sql_list():
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
