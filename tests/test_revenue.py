from __future__ import annotations

import sqlite3
from datetime import date

from turntabl.db import init_db
from turntabl.revenue import _allocation_revenue, client_revenue_year


def make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    init_db(conn)
    return conn


def test_allocation_revenue_open_ended_and_future_start():
    as_of = date(2026, 2, 15)

    # Open-ended allocation uses the as-of date for both to-date and total.
    open_ended = {
        "start_date": "2026-02-10",
        "end_date": None,
        "agreed_rate": None,
        "day_rate": 1000.0,
    }
    to_date, total = _allocation_revenue(open_ended, as_of)
    assert (to_date, total) == (6000.0, 6000.0)

    # Allocations starting after as-of should contribute no revenue.
    future = {
        "start_date": "2026-02-20",
        "end_date": "2026-02-25",
        "agreed_rate": 1500.0,
        "day_rate": 900.0,
    }
    assert _allocation_revenue(future, as_of) == (0.0, 0.0)


def test_client_revenue_year_splits_months_and_adds_total_row():
    conn = make_conn()
    conn.execute("INSERT INTO client (name) VALUES ('Acme')")
    conn.execute(
        "INSERT INTO project (client_id, name, start_date, end_date, agreed_rate) VALUES (1, 'Phoenix', '2026-01-01', '2026-12-31', 1200.0)"
    )
    conn.execute("INSERT INTO tlc_cohort (number) VALUES (1)")
    conn.execute("INSERT INTO engineer (name, level, day_rate, cohort_id) VALUES ('Ava', 3, 900.0, 1)")
    # Spans Jan->Feb: Jan has 12 days (20-31), Feb has 10 days (1-10).
    conn.execute(
        "INSERT INTO allocation (engineer_id, project_id, start_date, end_date) VALUES (1, 1, '2026-01-20', '2026-02-10')"
    )
    conn.commit()

    rows = client_revenue_year(conn, 2026)

    assert len(rows) == 2
    acme = rows[0]
    total = rows[1]

    assert acme["client"] == "Acme"
    assert acme["Jan"] == 14400.0
    assert acme["Feb"] == 12000.0
    assert acme["Mar"] == 0.0
    assert acme["total"] == 26400.0

    assert total["client"] == "TOTAL"
    assert total["Jan"] == 14400.0
    assert total["Feb"] == 12000.0
    assert total["total"] == 26400.0

    conn.close()
