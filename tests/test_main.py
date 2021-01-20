import pytest

import pymssqlutils as sql
from pymssqlutils import DatabaseResult


def test_with_conn_details_from_args():
    conn_details = sql.with_conn_details(
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
    conn_details = sql.with_conn_details({})
    assert conn_details["database"] == "database"
    assert conn_details["server"] == "server"
    assert conn_details["user"] == "user"
    assert conn_details["password"] == "password"


def test_build_conn_details_errors():
    with pytest.raises(EnvironmentError):
        sql.with_conn_details({})


def test_execute_and_execute_many(monkeypatch, mock_pymssql_connect):
    monkeypatch.setenv("DB_NAME", "database")
    monkeypatch.setenv("DB_SERVER", "server")
    monkeypatch.setenv("DB_USER", "user")
    monkeypatch.setenv("DB_PASSWORD", "password")

    result = sql.execute("test query")
    assert isinstance(result, DatabaseResult)
    assert result.data is None
    assert result.ok
    assert result.command == "execute"

    result = sql.execute_many("test query", [(1,), (2,)])
    assert isinstance(result, DatabaseResult)
    assert result.data is None
    assert result.ok
    assert result.command == "execute_many"


def test_query(monkeypatch, mock_pymssql_connect):
    monkeypatch.setenv("DB_NAME", "database")
    monkeypatch.setenv("DB_SERVER", "server")
    monkeypatch.setenv("DB_USER", "user")
    monkeypatch.setenv("DB_PASSWORD", "password")

    result = sql.query("test query")
    assert isinstance(result, DatabaseResult)
    assert result.data
    assert result.ok
    assert result.command == "query"

    # to test it is a named tuple
    assert isinstance(result.data[0], tuple)
    assert hasattr(result.data[0], "_asdict") and hasattr(result.data[0], "_fields")
