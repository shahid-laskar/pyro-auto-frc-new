"""Checkout liveness ping: dead idle connections are discarded, not handed out."""
from unittest.mock import MagicMock

import psycopg2
import pytest

from app.db import postgres


def _conn(alive=True, closed=0):
    conn = MagicMock()
    conn.closed = closed
    cur = MagicMock()
    if not alive:
        cur.execute.side_effect = psycopg2.OperationalError("server closed the connection unexpectedly")
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = None
    return conn


class FakePool:
    def __init__(self, conns, maxconn=10):
        self.conns = list(conns)
        self.maxconn = maxconn
        self.discarded = []

    def getconn(self):
        if not self.conns:
            raise psycopg2.OperationalError("could not connect to server")
        return self.conns.pop(0)

    def putconn(self, conn, close=False):
        if close:
            self.discarded.append(conn)


def test_live_connection_returned_after_ping():
    good = _conn()
    pool = FakePool([good])
    assert postgres._checkout_live_conn(pool, "write") is good
    good.rollback.assert_called_once()
    assert pool.discarded == []


def test_all_stale_idle_connections_skipped():
    dead = [_conn(alive=False) for _ in range(3)] + [_conn(closed=1)]
    good = _conn()
    pool = FakePool(dead + [good])
    assert postgres._checkout_live_conn(pool, "write") is good
    assert pool.discarded == dead


def test_database_down_raises_operational_error():
    pool = FakePool([_conn(alive=False)])
    with pytest.raises(psycopg2.OperationalError):
        postgres._checkout_live_conn(pool, "read")


def test_gives_up_after_bounded_attempts():
    pool = FakePool([_conn(alive=False) for _ in range(10)], maxconn=2)
    with pytest.raises(psycopg2.OperationalError, match="no live connection"):
        postgres._checkout_live_conn(pool, "write")
    assert len(pool.discarded) == 3
