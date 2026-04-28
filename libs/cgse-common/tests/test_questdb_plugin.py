import datetime

import pandas
import pytest

from egse.plugins.metrics.questdb import QuestDBRepository
from egse.plugins.metrics.questdb import get_repository_class


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.description = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def execute(self, query, params=None):
        self.conn.executed.append((query, params))
        self.description = [("col",)]

        if "SELECT 1" in query:
            self.conn.rows = [{"?column?": 1}]
        elif "SELECT table_name FROM tables()" in query:
            self.conn.rows = [{"table_name": "timeseries"}]
        elif "SHOW COLUMNS FROM" in query:
            self.conn.rows = [{"column": "measurement"}, {"column": "time"}, {"column": "fields"}]
        elif "SELECT time, fields" in query:
            self.conn.rows = [
                {
                    "time": datetime.datetime(2026, 4, 24, 12, 0, tzinfo=datetime.timezone.utc),
                    "fields": '{"temperature": 22.5}',
                }
            ]
        else:
            self.conn.rows = []
            self.description = None

    def executemany(self, query, rows):
        self.conn.executed_many.append((query, rows))

    def fetchall(self):
        return self.conn.rows

    def fetchone(self):
        if self.conn.rows:
            return self.conn.rows[0]
        return None


class FakeConnection:
    def __init__(self):
        self.executed = []
        self.executed_many = []
        self.rows = []
        self.closed = False

    def cursor(self):
        return FakeCursor(self)

    def close(self):
        self.closed = True


def test_connect_creates_table(monkeypatch):
    fake_conn = FakeConnection()

    def fake_connect(**kwargs):
        assert kwargs["host"] == "localhost"
        assert kwargs["port"] == 8812
        return fake_conn

    monkeypatch.setattr("egse.plugins.metrics.questdb.psycopg.connect", fake_connect)

    repo = QuestDBRepository()
    repo.connect()

    assert repo.conn is fake_conn
    assert any("CREATE TABLE IF NOT EXISTS timeseries" in query for query, _ in fake_conn.executed)


def test_ping_query_and_close(monkeypatch):
    fake_conn = FakeConnection()
    monkeypatch.setattr("egse.plugins.metrics.questdb.psycopg.connect", lambda **kwargs: fake_conn)

    repo = QuestDBRepository()
    repo.connect()

    assert repo.ping() is True

    rows = repo.query("SELECT table_name FROM tables()", mode="all")
    assert rows == [{"table_name": "timeseries"}]

    frame = repo.query("SELECT table_name FROM tables()", mode="pandas")
    assert isinstance(frame, pandas.DataFrame)
    assert list(frame["table_name"]) == ["timeseries"]

    with pytest.raises(ValueError, match="Invalid mode"):
        repo.query("SELECT 1", mode="unsupported")

    repo.close()
    assert fake_conn.closed is True


def test_write_and_helpers(monkeypatch):
    fake_conn = FakeConnection()
    monkeypatch.setattr("egse.plugins.metrics.questdb.psycopg.connect", lambda **kwargs: fake_conn)

    repo = QuestDBRepository(table_name="metrics")
    repo.connect()

    repo.write(
        [
            {
                "measurement": "camera_tm",
                "tags": {"device_id": "cam_01"},
                "fields": {"temperature": 23.4},
                "time": "2026-04-24T12:00:00Z",
            }
        ]
    )

    assert len(fake_conn.executed_many) == 1
    insert_query, rows = fake_conn.executed_many[0]
    assert "INSERT INTO metrics" in insert_query
    assert len(rows) == 1
    assert rows[0][0] == "camera_tm"

    assert repo.get_table_names() == ["timeseries"]
    assert repo.get_column_names("metrics") == ["measurement", "time", "fields"]

    values = repo.get_values_last_hours("metrics", "temperature", hours=1, mode="")
    assert len(values) == 1
    assert values[0]["temperature"] == 22.5

    values_range = repo.get_values_in_range("metrics", "temperature", "2026-04-24", "2026-04-25", mode="")
    assert len(values_range) == 1
    assert values_range[0]["temperature"] == 22.5


def test_get_repository_class():
    assert get_repository_class() is QuestDBRepository
