from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import sqlite3


@dataclass
class RevenueRow:
    id: int
    name: str
    revenue_to_date: float
    revenue_total: float


def _effective_rate(row: sqlite3.Row) -> float | None:
    return row["agreed_rate"] if row["agreed_rate"] is not None else row["day_rate"]


def _days_inclusive(start: date, end: date) -> int:
    return (end - start).days + 1


def _allocation_revenue(row: sqlite3.Row, as_of: date) -> tuple[float, float]:
    rate = _effective_rate(row)
    if rate is None:
        return 0.0, 0.0
    start = date.fromisoformat(row["start_date"])
    end_raw = row["end_date"]
    if start > as_of:
        return 0.0, 0.0
    if end_raw is None:
        end_for_total = as_of
    else:
        end_for_total = date.fromisoformat(end_raw)
    end_for_to_date = min(end_for_total, as_of)
    if end_for_to_date < start:
        return 0.0, 0.0
    to_date = _days_inclusive(start, end_for_to_date) * rate
    total = _days_inclusive(start, end_for_total) * rate
    return to_date, total


def project_revenue(conn: sqlite3.Connection, as_of: date) -> list[dict]:
    cur = conn.execute(
        """
        SELECT p.id, p.name, a.start_date, a.end_date, p.agreed_rate, e.day_rate
        FROM project p
        LEFT JOIN allocation a ON a.project_id = p.id
        LEFT JOIN engineer e ON e.id = a.engineer_id
        ORDER BY p.name ASC
        """
    )
    rows = cur.fetchall()
    buckets: dict[int, RevenueRow] = {}
    for row in rows:
        pid = row["id"]
        if pid not in buckets:
            buckets[pid] = RevenueRow(id=pid, name=row["name"], revenue_to_date=0.0, revenue_total=0.0)
        if row["start_date"] is None:
            continue
        to_date, total = _allocation_revenue(row, as_of)
        buckets[pid].revenue_to_date += to_date
        buckets[pid].revenue_total += total
    return [
        {
            "project_id": r.id,
            "project": r.name,
            "revenue_to_date": round(r.revenue_to_date, 2),
            "revenue_total": round(r.revenue_total, 2),
        }
        for r in buckets.values()
    ]


def client_revenue(conn: sqlite3.Connection, as_of: date) -> list[dict]:
    cur = conn.execute(
        """
        SELECT c.id AS client_id, c.name AS client, a.start_date, a.end_date, p.agreed_rate, e.day_rate
        FROM client c
        LEFT JOIN project p ON p.client_id = c.id
        LEFT JOIN allocation a ON a.project_id = p.id
        LEFT JOIN engineer e ON e.id = a.engineer_id
        ORDER BY c.name ASC
        """
    )
    rows = cur.fetchall()
    buckets: dict[int, RevenueRow] = {}
    for row in rows:
        cid = row["client_id"]
        if cid not in buckets:
            buckets[cid] = RevenueRow(id=cid, name=row["client"], revenue_to_date=0.0, revenue_total=0.0)
        if row["start_date"] is None:
            continue
        to_date, total = _allocation_revenue(row, as_of)
        buckets[cid].revenue_to_date += to_date
        buckets[cid].revenue_total += total
    return [
        {
            "client_id": r.id,
            "client": r.name,
            "revenue_to_date": round(r.revenue_to_date, 2),
            "revenue_total": round(r.revenue_total, 2),
        }
        for r in buckets.values()
    ]


def engineer_revenue(conn: sqlite3.Connection, as_of: date) -> list[dict]:
    cur = conn.execute(
        """
        SELECT e.id AS engineer_id, e.name AS engineer, a.start_date, a.end_date, p.agreed_rate, e.day_rate
        FROM engineer e
        LEFT JOIN allocation a ON a.engineer_id = e.id
        LEFT JOIN project p ON p.id = a.project_id
        ORDER BY e.name ASC
        """
    )
    rows = cur.fetchall()
    buckets: dict[int, RevenueRow] = {}
    for row in rows:
        eid = row["engineer_id"]
        if eid not in buckets:
            buckets[eid] = RevenueRow(id=eid, name=row["engineer"], revenue_to_date=0.0, revenue_total=0.0)
        if row["start_date"] is None:
            continue
        to_date, total = _allocation_revenue(row, as_of)
        buckets[eid].revenue_to_date += to_date
        buckets[eid].revenue_total += total
    return [
        {
            "engineer_id": r.id,
            "engineer": r.name,
            "revenue_to_date": round(r.revenue_to_date, 2),
            "revenue_total": round(r.revenue_total, 2),
        }
        for r in buckets.values()
    ]


def client_revenue_year(conn: sqlite3.Connection, year: int) -> list[dict]:
    start_year = date(year, 1, 1)
    end_year = date(year, 12, 31)
    cur = conn.execute(
        """
        SELECT c.id AS client_id, c.name AS client,
               a.start_date, a.end_date, p.agreed_rate, e.day_rate
        FROM client c
        LEFT JOIN project p ON p.client_id = c.id
        LEFT JOIN allocation a ON a.project_id = p.id
        LEFT JOIN engineer e ON e.id = a.engineer_id
        ORDER BY c.name ASC
        """
    )
    rows = cur.fetchall()
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    buckets: dict[int, dict] = {}

    def days_overlap(start: date, end: date, window_start: date, window_end: date) -> int:
        if end < window_start or start > window_end:
            return 0
        s = max(start, window_start)
        e = min(end, window_end)
        return (e - s).days + 1

    for row in rows:
        cid = row["client_id"]
        if cid not in buckets:
            buckets[cid] = {"client_id": cid, "client": row["client"], **{m: 0.0 for m in months}, "total": 0.0}
        if row["start_date"] is None:
            continue
        rate = _effective_rate(row)
        if rate is None:
            continue
        alloc_start = date.fromisoformat(row["start_date"])
        alloc_end = date.fromisoformat(row["end_date"]) if row["end_date"] else end_year
        # clamp to year
        if alloc_end < start_year or alloc_start > end_year:
            continue
        for month_idx in range(1, 13):
            m_start = date(year, month_idx, 1)
            if month_idx == 12:
                m_end = date(year, 12, 31)
            else:
                m_end = date(year, month_idx + 1, 1).fromordinal(date(year, month_idx + 1, 1).toordinal() - 1)
            days = days_overlap(alloc_start, alloc_end, m_start, m_end)
            if days:
                amount = days * rate
                key = months[month_idx - 1]
                buckets[cid][key] += amount
                buckets[cid]["total"] += amount

    result = []
    for row in buckets.values():
        for m in months:
            row[m] = round(row[m], 2)
        row["total"] = round(row["total"], 2)
        result.append(row)
    # totals row
    if result:
        totals = {"client_id": None, "client": "TOTAL", **{m: 0.0 for m in months}, "total": 0.0}
        for row in result:
            for m in months:
                totals[m] += row[m]
            totals["total"] += row["total"]
        for m in months:
            totals[m] = round(totals[m], 2)
        totals["total"] = round(totals["total"], 2)
        result.append(totals)
    return result
