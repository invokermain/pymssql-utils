import pymssql
import pytest
from pytest_mock import MockerFixture

import pymssqlutils as sql
from pymssqlutils import DatabaseResult


@pytest.fixture(autouse=True)
def set_env(monkeypatch):
    monkeypatch.setenv("MSSQL_SERVER", "server")


def test_execute_single_operations_no_params(mocker: MockerFixture, monkeypatch):
    conn = mocker.patch("pymssqlutils.methods._get_connection", autospec=True)
    cursor = (
        conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
    )
    result = sql.execute("test query")
    assert cursor.execute.call_args_list == [(("test query",),)]
    assert isinstance(result, DatabaseResult)
    with pytest.raises(ValueError):
        result.data
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
    with pytest.raises(ValueError):
        result.data
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
    with pytest.raises(ValueError):
        result.data
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
    with pytest.raises(ValueError):
        result.data
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
    with pytest.raises(ValueError):
        result.data
    assert result.ok
    assert result.commit


def test_execute_multiple_operations_no_params_batched(
    mocker: MockerFixture, monkeypatch
):
    monkeypatch.setenv("MSSQL_SERVER", "server")
    conn = mocker.patch("pymssqlutils.methods._get_connection", autospec=True)
    cursor = (
        conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
    )
    result = sql.execute(["test query", "second query"] * 2, batch_size=2)
    assert cursor.execute.call_args_list == [
        (("test query\n;second query",),),
        (("test query\n;second query",),),
    ]
    assert isinstance(result, DatabaseResult)
    with pytest.raises(ValueError):
        result.data
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
    with pytest.raises(ValueError):
        result.data
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
    with pytest.raises(ValueError):
        result.data
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
    with pytest.raises(ValueError):
        result.data
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
    with pytest.raises(ValueError):
        result.data
    assert result.ok
    assert result.commit


def test_query(mocker: MockerFixture, monkeypatch):
    monkeypatch.setenv("MSSQL_SERVER", "server")
    mocker.patch(
        "pymssqlutils.databaseresult._get_result_sets",
        return_value=(([(1,)], ("Col1",), (4,)),),
    ),
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


def test_multiset_query(mocker: MockerFixture, monkeypatch):
    monkeypatch.setenv("MSSQL_SERVER", "server")
    mocker.patch(
        "pymssqlutils.databaseresult._get_result_sets",
        return_value=(([(1,)], ("Col1",), (4,)),),
    ),
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


def test_result_error_handling_on_ignore(mocker):
    mocker.patch("pymssqlutils.methods._execute", side_effect=pymssql.OperationalError)
    result = sql.query(
        "test query",
        raise_errors=False,
    )
    assert result.ok is False
    assert isinstance(result.error, pymssql.OperationalError)
    with pytest.raises(ValueError):
        result.data


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
