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


def report_everything_year(conn: sqlite3.Connection, year: int, include_provisional: bool = True) -> list[dict]:
    start_year = date(year, 1, 1)
    end_year = date(year, 12, 31)
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

    def days_overlap(start: date, end: date, window_start: date, window_end: date) -> int:
        if end < window_start or start > window_end:
            return 0
        s = max(start, window_start)
        e = min(end, window_end)
        return (e - s).days + 1

    cur = conn.execute(
        """
        SELECT
            c.id AS client_id,
            c.name AS client,
            p.id AS project_id,
            p.name AS project,
            p.status AS project_status,
            p.agreed_rate,
            a.id AS allocation_id,
            a.start_date,
            a.end_date,
            a.status AS alloc_status,
            e.name AS engineer,
            e.day_rate
        FROM client c
        LEFT JOIN project p ON p.client_id = c.id
        LEFT JOIN allocation a ON a.project_id = p.id
        LEFT JOIN engineer e ON e.id = a.engineer_id
        ORDER BY c.name ASC, p.name ASC, a.start_date ASC, a.id ASC
        """
    )
    rows = cur.fetchall()

    client_rows: dict[int, dict] = {}
    project_rows: dict[int, dict] = {}
    allocation_rows: list[dict] = []

    for row in rows:
        client_id = row["client_id"]
        if client_id not in client_rows:
            client_rows[client_id] = {
                "row_id": f"client:{client_id}",
                "parent_id": "",
                "row_type": "client",
                "label": row["client"],
                "at_risk": 0,
                "expandable": 0,
                **{m: 0.0 for m in months},
                "total": 0.0,
            }

        project_id = row["project_id"]
        if project_id is None:
            continue
        if not include_provisional and row["project_status"] != "confirmed":
            continue

        if project_id not in project_rows:
            project_rows[project_id] = {
                "row_id": f"project:{project_id}",
                "parent_id": f"client:{client_id}",
                "row_type": "project",
                "label": row["project"],
                "at_risk": 1 if row["project_status"] == "provisional" else 0,
                "expandable": 0,
                **{m: 0.0 for m in months},
                "total": 0.0,
            }

        if row["allocation_id"] is None:
            continue
        if not include_provisional and row["alloc_status"] != "confirmed":
            continue

        alloc_start = date.fromisoformat(row["start_date"])
        alloc_end = date.fromisoformat(row["end_date"]) if row["end_date"] else end_year
        if alloc_end < start_year or alloc_start > end_year:
            continue
        rate = row["agreed_rate"] if row["agreed_rate"] is not None else row["day_rate"]
        if rate is None:
            continue

        alloc_row = {
            "row_id": f"allocation:{row['allocation_id']}",
            "parent_id": f"project:{project_id}",
            "row_type": "allocation",
            "label": f"{row['engineer']} {row['start_date']}->{row['end_date'] or 'open'}",
            "at_risk": 1 if row["project_status"] == "provisional" or row["alloc_status"] == "provisional" else 0,
            "expandable": 0,
            **{m: 0.0 for m in months},
            "total": 0.0,
        }

        for month_idx in range(1, 13):
            m_start = date(year, month_idx, 1)
            if month_idx == 12:
                m_end = date(year, 12, 31)
            else:
                m_end = date(year, month_idx + 1, 1).fromordinal(date(year, month_idx + 1, 1).toordinal() - 1)
            days = days_overlap(alloc_start, alloc_end, m_start, m_end)
            if not days:
                continue
            amount = round(days * rate, 2)
            key = months[month_idx - 1]
            alloc_row[key] += amount
            alloc_row["total"] += amount
            project_rows[project_id][key] += amount
            project_rows[project_id]["total"] += amount
            client_rows[client_id][key] += amount
            client_rows[client_id]["total"] += amount

        allocation_rows.append(alloc_row)

    client_has_projects = {row["parent_id"]: 0 for row in project_rows.values()}
    for project in project_rows.values():
        client_has_projects[project["parent_id"]] = 1
    for client in client_rows.values():
        client["expandable"] = client_has_projects.get(client["row_id"], 0)
    project_has_allocations = {row["parent_id"]: 0 for row in allocation_rows}
    for alloc in allocation_rows:
        project_has_allocations[alloc["parent_id"]] = 1
    for project in project_rows.values():
        project["expandable"] = project_has_allocations.get(project["row_id"], 0)

    result: list[dict] = []
    for client in sorted(client_rows.values(), key=lambda r: r["label"].lower()):
        result.append({**client, **{m: round(client[m], 2) for m in months}, "total": round(client["total"], 2)})
        projects = sorted(
            [p for p in project_rows.values() if p["parent_id"] == client["row_id"]], key=lambda r: r["label"].lower()
        )
        for project in projects:
            result.append({**project, **{m: round(project[m], 2) for m in months}, "total": round(project["total"], 2)})
            allocations = [a for a in allocation_rows if a["parent_id"] == project["row_id"]]
            for alloc in allocations:
                result.append({**alloc, **{m: round(alloc[m], 2) for m in months}, "total": round(alloc["total"], 2)})

    if result:
        totals = {
            "row_id": "total",
            "parent_id": "",
            "row_type": "total",
            "label": "TOTAL",
            "at_risk": 0,
            "expandable": 0,
            **{m: 0.0 for m in months},
            "total": 0.0,
        }
        for row in result:
            if row["row_type"] != "client":
                continue
            for month in months:
                totals[month] += row[month]
            totals["total"] += row["total"]
        for month in months:
            totals[month] = round(totals[month], 2)
        totals["total"] = round(totals["total"], 2)
        result.append(totals)

    return result


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
