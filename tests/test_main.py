from datetime import date, time, datetime
from typing import Dict

import pymssql
import pytest

import pymssqlutils as sql
from pymssqlutils.methods import with_conn_details
from pymssqlutils import DatabaseResult


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
    monkeypatch.setenv("DB_NAME", "database")
    monkeypatch.setenv("DB_SERVER", "server")
    monkeypatch.setenv("DB_USER", "user")
    monkeypatch.setenv("DB_PASSWORD", "password")
    conn_details = with_conn_details({})
    assert conn_details["database"] == "database"
    assert conn_details["server"] == "server"
    assert conn_details["user"] == "user"
    assert conn_details["password"] == "password"


def test_build_conn_details_errors():
    with pytest.raises(EnvironmentError):
        with_conn_details({})


def test_execute_and_execute_many(monkeypatch, mock_pymssql_connect):
    monkeypatch.setenv("DB_NAME", "database")
    monkeypatch.setenv("DB_SERVER", "server")
    monkeypatch.setenv("DB_USER", "user")
    monkeypatch.setenv("DB_PASSWORD", "password")

    result = sql.execute("test query")
    assert isinstance(result, DatabaseResult)
    assert result.data is None
    assert result.ok
    assert "commit" in result.execution_args

    result = sql.execute_many("test query", [(1,), (2,)])
    assert isinstance(result, DatabaseResult)
    assert result.data is None
    assert result.ok
    assert "commit" in result.execution_args and "many" in result.execution_args


def test_query(monkeypatch, mock_pymssql_connect):
    monkeypatch.setenv("DB_NAME", "database")
    monkeypatch.setenv("DB_SERVER", "server")
    monkeypatch.setenv("DB_USER", "user")
    monkeypatch.setenv("DB_PASSWORD", "password")

    result = sql.query("test query")
    assert isinstance(result, DatabaseResult)
    assert result.data
    assert result.ok
    assert "fetch" in result.execution_args

    assert isinstance(result.data[0], Dict)
    assert result.data[0]["Col_Date"]


def test_data_parsing(monkeypatch, mock_pymssql_connect):
    monkeypatch.setenv("DB_NAME", "database")
    monkeypatch.setenv("DB_SERVER", "server")
    monkeypatch.setenv("DB_USER", "user")
    monkeypatch.setenv("DB_PASSWORD", "password")

    data = sql.query("test query").data[0]

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


def test_data_serializable(monkeypatch, mock_pymssql_connect):
    monkeypatch.setenv("DB_NAME", "database")
    monkeypatch.setenv("DB_SERVER", "server")
    monkeypatch.setenv("DB_USER", "user")
    monkeypatch.setenv("DB_PASSWORD", "password")

    result = sql.query("test query")

    assert isinstance(result.to_json(), str)
    assert isinstance(result.to_json(as_bytes=True), bytes)


def test_result_error_handling_on_ignore(monkeypatch):
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
