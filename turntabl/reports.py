from __future__ import annotations

from datetime import date, timedelta
import sqlite3

from .db import rows_to_dicts
from .revenue import client_revenue, engineer_revenue, project_revenue, client_revenue_year


def unallocated_engineers(conn: sqlite3.Connection, as_of: date, include_provisional: bool = True) -> list[dict]:
    cur = conn.execute(
        """
        SELECT e.id, e.name, e.level, e.day_rate, c.number AS cohort
        FROM engineer e
        JOIN tlc_cohort c ON c.id = e.cohort_id
        WHERE e.active = 1
          AND e.id NOT IN (
              SELECT a.engineer_id
              FROM allocation a
              JOIN project p ON p.id = a.project_id
              WHERE a.start_date <= ? AND (a.end_date IS NULL OR a.end_date >= ?)
                AND (? = 1 OR (a.status = 'confirmed' AND p.status = 'confirmed'))
          )
        ORDER BY e.name;
        """,
        (as_of.isoformat(), as_of.isoformat(), 1 if include_provisional else 0),
    )
    return rows_to_dicts(cur.fetchall())


def projects_ending_soon(
    conn: sqlite3.Connection, as_of: date, within_days: int, include_provisional: bool = True
) -> list[dict]:
    end_by = as_of + timedelta(days=within_days)
    cur = conn.execute(
        """
        SELECT p.id, p.name, p.end_date, p.status, c.name AS client
        FROM project p
        JOIN client c ON c.id = p.client_id
        WHERE p.end_date IS NOT NULL AND p.end_date >= ? AND p.end_date <= ?
          AND (? = 1 OR p.status = 'confirmed')
        ORDER BY p.end_date ASC;
        """,
        (as_of.isoformat(), end_by.isoformat(), 1 if include_provisional else 0),
    )
    return rows_to_dicts(cur.fetchall())


def projects_with_no_allocations(conn: sqlite3.Connection, include_provisional: bool = True) -> list[dict]:
    cur = conn.execute(
        """
        SELECT p.id, p.name, p.start_date, p.end_date, p.status, c.name AS client
        FROM project p
        JOIN client c ON c.id = p.client_id
        LEFT JOIN allocation a ON a.project_id = p.id
        WHERE a.id IS NULL AND (? = 1 OR p.status = 'confirmed')
        ORDER BY p.start_date ASC;
        """,
        (1 if include_provisional else 0,),
    )
    return rows_to_dicts(cur.fetchall())


def current_allocations(conn: sqlite3.Connection, as_of: date, include_provisional: bool = True) -> list[dict]:
    cur = conn.execute(
        """
        SELECT
            e.id AS engineer_id,
            e.name AS engineer,
            e.level,
            e.day_rate,
            p.id AS project_id,
            p.name AS project,
            c.name AS client,
            p.agreed_rate,
            a.start_date,
            a.end_date,
            a.status AS allocation_status,
            p.status AS project_status,
            CASE
                WHEN p.agreed_rate IS NOT NULL THEN p.agreed_rate
                ELSE e.day_rate
            END AS effective_rate
        FROM allocation a
        JOIN engineer e ON e.id = a.engineer_id
        JOIN project p ON p.id = a.project_id
        JOIN client c ON c.id = p.client_id
        WHERE a.start_date <= ? AND (a.end_date IS NULL OR a.end_date >= ?)
          AND (? = 1 OR (a.status = 'confirmed' AND p.status = 'confirmed'))
        ORDER BY e.name;
        """,
        (as_of.isoformat(), as_of.isoformat(), 1 if include_provisional else 0),
    )
    return rows_to_dicts(cur.fetchall())


def report_project_revenue(conn: sqlite3.Connection, as_of: date, include_provisional: bool = True) -> list[dict]:
    return project_revenue(conn, as_of, include_provisional)


def report_client_revenue(conn: sqlite3.Connection, as_of: date, include_provisional: bool = True) -> list[dict]:
    return client_revenue(conn, as_of, include_provisional)


def report_engineer_revenue(conn: sqlite3.Connection, as_of: date, include_provisional: bool = True) -> list[dict]:
    return engineer_revenue(conn, as_of, include_provisional)


def report_client_revenue_year(conn: sqlite3.Connection, year: int, include_provisional: bool = True) -> list[dict]:
    return client_revenue_year(conn, year, include_provisional)


def projects_ending_with_details(
    conn: sqlite3.Connection, as_of: date, within_days: int, include_provisional: bool = True
) -> list[dict]:
    end_by = as_of + timedelta(days=within_days)
    cur = conn.execute(
        """
        SELECT
            p.id AS project_id,
            p.name AS project,
            p.end_date,
            p.status AS project_status,
            c.name AS client,
            a.id AS allocation_id,
            a.start_date AS alloc_start,
            a.end_date AS alloc_end,
            a.status AS alloc_status,
            e.name AS engineer,
            p.agreed_rate,
            e.day_rate
        FROM project p
        JOIN client c ON c.id = p.client_id
        LEFT JOIN allocation a ON a.project_id = p.id
        LEFT JOIN engineer e ON e.id = a.engineer_id
        WHERE p.end_date IS NOT NULL AND p.end_date >= ? AND p.end_date <= ?
          AND (? = 1 OR p.status = 'confirmed')
        ORDER BY p.end_date ASC, p.name ASC, a.start_date ASC
        """,
        (as_of.isoformat(), end_by.isoformat(), 1 if include_provisional else 0),
    )
    rows = cur.fetchall()
    projects: dict[int, dict] = {}

    def allocation_revenue(start: str, end: str | None, rate: float | None) -> tuple[float, float]:
        if rate is None:
            return 0.0, 0.0
        s = date.fromisoformat(start)
        e = date.fromisoformat(end) if end else as_of
        if s > as_of:
            return 0.0, 0.0
        to_date_end = min(e, as_of)
        if to_date_end < s:
            return 0.0, 0.0
        days_to_date = (to_date_end - s).days + 1
        days_total = (e - s).days + 1
        return days_to_date * rate, days_total * rate

    for row in rows:
        pid = row["project_id"]
        if pid not in projects:
            projects[pid] = {
                "project_id": pid,
                "project": row["project"],
                "client": row["client"],
                "end_date": row["end_date"],
                "allocations": [],
                "revenue_to_date": 0.0,
                "revenue_total": 0.0,
            }
        if row["allocation_id"] is None:
            continue
        if not include_provisional and row["alloc_status"] != "confirmed":
            continue
        end_label = row["alloc_end"] or "open"
        alloc_label = f"{row['engineer']} {row['alloc_start']}->{end_label}"
        projects[pid]["allocations"].append(alloc_label)
        rate = row["agreed_rate"] if row["agreed_rate"] is not None else row["day_rate"]
        to_date, total = allocation_revenue(row["alloc_start"], row["alloc_end"], rate)
        projects[pid]["revenue_to_date"] += to_date
        projects[pid]["revenue_total"] += total

    result = []
    for proj in projects.values():
        result.append(
            {
                "project": proj["project"],
                "client": proj["client"],
                "end_date": proj["end_date"],
                "allocations": "; ".join(proj["allocations"]) if proj["allocations"] else "(none)",
                "revenue_to_date": round(proj["revenue_to_date"], 2),
                "revenue_total": round(proj["revenue_total"], 2),
            }
        )
    return result
