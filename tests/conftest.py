from contextlib import contextmanager

import pytest
import pymssql

from tests.helpers import MockConnection


@pytest.fixture(autouse=False)
def mock_pymssql_connect(monkeypatch):
    @contextmanager
    def mock_connect(as_dict=False, **kwargs):
        yield MockConnection(as_dict=as_dict)

    monkeypatch.setattr(pymssql, "connect", mock_connect)
