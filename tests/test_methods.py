from datetime import datetime, timedelta, timezone

from pymssqlutils import (
    model_to_values,
    set_connection_details,
    substitute_parameters,
    to_sql_list,
)
from pymssqlutils.methods import _with_conn_details


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
