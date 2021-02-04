from contextlib import contextmanager

import pymssql
import pytest

from tests.helpers import MockConnection


@pytest.fixture()
def mock_pymssql_connect(monkeypatch):
    @contextmanager
    def mock_connect(as_dict, **kwargs):
        yield MockConnection(as_dict=as_dict)

    monkeypatch.setattr(pymssql, "connect", mock_connect)
    monkeypatch.setenv("MSSQL_SERVER", "test_server")
