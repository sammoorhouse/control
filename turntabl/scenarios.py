from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
import sqlite3

from .revenue import client_revenue_year


@dataclass
class Scenario:
    id: int
    name: str
    created_at: str


def list_scenarios(conn: sqlite3.Connection) -> list[Scenario]:
    cur = conn.execute("SELECT id, name, created_at FROM scenario ORDER BY created_at DESC")
    return [Scenario(id=row["id"], name=row["name"], created_at=row["created_at"]) for row in cur.fetchall()]


def create_scenario(conn: sqlite3.Connection, name: str) -> Scenario:
    now = datetime.utcnow().isoformat()
    cur = conn.execute(
        "INSERT INTO scenario (name, created_at) VALUES (?, ?)",
        (name, now),
    )
    conn.commit()
    return Scenario(id=cur.lastrowid, name=name, created_at=now)


def add_change(conn: sqlite3.Connection, scenario_id: int, change_type: str, payload: dict) -> None:
    now = datetime.utcnow().isoformat()
    conn.execute(
        "INSERT INTO scenario_change (scenario_id, change_type, payload, created_at) VALUES (?, ?, ?, ?)",
        (scenario_id, change_type, json.dumps(payload), now),
    )
    conn.commit()


def list_changes(conn: sqlite3.Connection, scenario_id: int) -> list[dict]:
    cur = conn.execute(
        "SELECT id, change_type, payload, created_at FROM scenario_change WHERE scenario_id = ? ORDER BY created_at ASC",
        (scenario_id,),
    )
    return [
        {
            "id": row["id"],
            "change_type": row["change_type"],
            "payload": json.loads(row["payload"]),
            "created_at": row["created_at"],
        }
        for row in cur.fetchall()
    ]


def _load_base_data(conn: sqlite3.Connection) -> dict:
    clients = [dict(row) for row in conn.execute("SELECT id, name FROM client")]
    projects = [
        dict(row)
        for row in conn.execute(
            "SELECT id, client_id, name, start_date, end_date, agreed_rate FROM project"
        )
    ]
    engineers = [dict(row) for row in conn.execute("SELECT id, name, day_rate FROM engineer")]
    allocations = [
        dict(row)
        for row in conn.execute(
            "SELECT id, engineer_id, project_id, start_date, end_date FROM allocation"
        )
    ]
    return {
        "clients": clients,
        "projects": projects,
        "engineers": engineers,
        "allocations": allocations,
    }


def _index_by_id(items: list[dict]) -> dict[int, dict]:
    return {int(item["id"]): item for item in items}


def _apply_changes(base: dict, changes: list[dict]) -> dict:
    data = {
        "clients": [dict(x) for x in base["clients"]],
        "projects": [dict(x) for x in base["projects"]],
        "engineers": [dict(x) for x in base["engineers"]],
        "allocations": [dict(x) for x in base["allocations"]],
    }

    next_project_id = (max([p["id"] for p in data["projects"]], default=0) + 1)
    next_alloc_id = (max([a["id"] for a in data["allocations"]], default=0) + 1)

    for change in changes:
        ctype = change["change_type"]
        payload = change["payload"]
        if ctype == "engineer_rate":
            for eng in data["engineers"]:
                if eng["id"] == payload["engineer_id"]:
                    eng["day_rate"] = payload["new_day_rate"]
        elif ctype == "project_add":
            proj_id = payload.get("project_id", next_project_id)
            proj = {
                "id": proj_id,
                "client_id": payload["client_id"],
                "name": payload["name"],
                "start_date": payload["start_date"],
                "end_date": payload.get("end_date"),
                "agreed_rate": payload.get("agreed_rate"),
                "tentative": 1,
            }
            next_project_id = max(next_project_id, proj_id + 1)
            data["projects"].append(proj)
        elif ctype == "project_update":
            for proj in data["projects"]:
                if proj["id"] == payload["project_id"]:
                    for key in ("name", "start_date", "end_date", "agreed_rate", "client_id"):
                        if key in payload:
                            proj[key] = payload[key]
        elif ctype == "project_delete":
            data["projects"] = [p for p in data["projects"] if p["id"] != payload["project_id"]]
            data["allocations"] = [a for a in data["allocations"] if a["project_id"] != payload["project_id"]]
        elif ctype == "allocation_add":
            alloc = {
                "id": next_alloc_id,
                "engineer_id": payload["engineer_id"],
                "project_id": payload["project_id"],
                "start_date": payload["start_date"],
                "end_date": payload.get("end_date"),
            }
            next_alloc_id += 1
            data["allocations"].append(alloc)
        elif ctype == "allocation_update":
            for alloc in data["allocations"]:
                if alloc["id"] == payload["allocation_id"]:
                    for key in ("engineer_id", "project_id", "start_date", "end_date"):
                        if key in payload:
                            alloc[key] = payload[key]
        elif ctype == "allocation_delete":
            data["allocations"] = [a for a in data["allocations"] if a["id"] != payload["allocation_id"]]

    return data


def _client_revenue_year_from_data(data: dict, year: int) -> list[dict]:
    # Reuse the existing DB-based function by reconstructing via sqlite in memory is overkill.
    # Compute directly from in-memory data.
    start_year = date(year, 1, 1)
    end_year = date(year, 12, 31)
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

    clients = _index_by_id(data["clients"])
    projects = _index_by_id(data["projects"])
    engineers = _index_by_id(data["engineers"])

    buckets: dict[int, dict] = {}

    def days_overlap(start: date, end: date, window_start: date, window_end: date) -> int:
        if end < window_start or start > window_end:
            return 0
        s = max(start, window_start)
        e = min(end, window_end)
        return (e - s).days + 1

    for alloc in data["allocations"]:
        proj = projects.get(alloc["project_id"])
        if not proj:
            continue
        client_id = proj["client_id"]
        client_name = clients.get(client_id, {}).get("name", "Unknown")
        if client_id not in buckets:
            buckets[client_id] = {
                "client_id": client_id,
                "client": client_name,
                **{m: 0.0 for m in months},
                "total": 0.0,
            }
        rate = proj.get("agreed_rate")
        if rate is None:
            eng = engineers.get(alloc["engineer_id"])
            rate = eng.get("day_rate") if eng else None
        if rate is None:
            continue
        alloc_start = date.fromisoformat(alloc["start_date"])
        alloc_end = date.fromisoformat(alloc["end_date"]) if alloc.get("end_date") else end_year
        if alloc_end < start_year or alloc_start > end_year:
            continue
        for month_idx in range(1, 13):
            m_start = date(year, month_idx, 1)
            m_end = date(year, 12, 31) if month_idx == 12 else date(year, month_idx + 1, 1).fromordinal(
                date(year, month_idx + 1, 1).toordinal() - 1
            )
            days = days_overlap(alloc_start, alloc_end, m_start, m_end)
            if days:
                amount = days * rate
                key = months[month_idx - 1]
                buckets[client_id][key] += amount
                buckets[client_id]["total"] += amount

    result = []
    for row in buckets.values():
        for m in months:
            row[m] = round(row[m], 2)
        row["total"] = round(row["total"], 2)
        result.append(row)

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


def scenario_client_revenue_year(conn: sqlite3.Connection, scenario_id: int, year: int) -> dict:
    base = _load_base_data(conn)
    changes = list_changes(conn, scenario_id)
    scenario = _apply_changes(base, changes)

    base_rows = client_revenue_year(conn, year)
    scenario_rows = _client_revenue_year_from_data(scenario, year)

    # Apply cell adjustments
    months = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"}
    for change in changes:
        if change["change_type"] != "cell_adjust":
            continue
        payload = change["payload"]
        client_id = payload["client_id"]
        month = payload["month"]
        amount = float(payload["amount"])
        if month not in months and month != "total":
            continue
        for row in scenario_rows:
            if row["client_id"] == client_id:
                row[month] = round(row.get(month, 0.0) + amount, 2)
                if month != "total":
                    row["total"] = round(row.get("total", 0.0) + amount, 2)
                break
        # Update TOTAL row as well
        for row in scenario_rows:
            if row.get("client") == "TOTAL":
                row[month] = round(row.get(month, 0.0) + amount, 2)
                if month != "total":
                    row["total"] = round(row.get("total", 0.0) + amount, 2)
                break

    base_map = {row["client_id"]: row for row in base_rows}
    scen_map = {row["client_id"]: row for row in scenario_rows}

    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec", "total"]
    dirty = {}
    for client_id, row in scen_map.items():
        base_row = base_map.get(client_id, {})
        for m in months:
            if row.get(m) != base_row.get(m):
                dirty.setdefault(client_id, set()).add(m)
    if None in scen_map:
        base_total = base_map.get(None, {})
        scen_total = scen_map.get(None, {})
        for m in months:
            if scen_total.get(m) != base_total.get(m):
                dirty.setdefault(None, set()).add(m)

    cell_changes = _scenario_cell_changes(base, scenario, changes, year)

    return {
        "rows": scenario_rows,
        "dirty": dirty,
        "cell_changes": cell_changes,
    }


def _scenario_cell_changes(base: dict, scenario: dict, changes: list[dict], year: int) -> dict:
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    start_year = date(year, 1, 1)
    end_year = date(year, 12, 31)

    clients = _index_by_id(scenario["clients"])
    projects = _index_by_id(scenario["projects"])
    engineers = _index_by_id(scenario["engineers"])

    base_projects = _index_by_id(base["projects"])
    base_allocs = _index_by_id(base["allocations"])

    def months_for_alloc(alloc: dict, proj: dict) -> set[str]:
        alloc_start = date.fromisoformat(alloc["start_date"])
        alloc_end = date.fromisoformat(alloc["end_date"]) if alloc.get("end_date") else end_year
        if alloc_end < start_year or alloc_start > end_year:
            return set()
        result = set()
        for month_idx in range(1, 13):
            m_start = date(year, month_idx, 1)
            m_end = date(year, 12, 31) if month_idx == 12 else date(year, month_idx + 1, 1).fromordinal(
                date(year, month_idx + 1, 1).toordinal() - 1
            )
            if alloc_end < m_start or alloc_start > m_end:
                continue
            result.add(months[month_idx - 1])
        return result

    def add_changes(client_id: int | None, month_keys: set[str], desc: str, mapping: dict):
        if client_id is None:
            return
        for m in month_keys:
            mapping.setdefault((client_id, m), []).append(desc)
        if month_keys:
            mapping.setdefault((client_id, "total"), []).append(desc)
        for m in month_keys:
            mapping.setdefault((None, m), []).append(desc)
        if month_keys:
            mapping.setdefault((None, "total"), []).append(desc)

    mapping: dict[tuple[int | None, str], list[str]] = {}

    for change in changes:
        ctype = change["change_type"]
        payload = change["payload"]
        if ctype == "engineer_rate":
            eng = engineers.get(payload["engineer_id"])
            name = eng.get("name") if eng else f"engineer {payload['engineer_id']}"
            desc = f"Engineer rate change: {name} -> {payload['new_day_rate']}"
            for alloc in scenario["allocations"]:
                if alloc["engineer_id"] != payload["engineer_id"]:
                    continue
                proj = projects.get(alloc["project_id"])
                if not proj:
                    continue
                client_id = proj["client_id"]
                add_changes(client_id, months_for_alloc(alloc, proj), desc, mapping)
        elif ctype == "allocation_add":
            proj = projects.get(payload["project_id"])
            if not proj:
                continue
            eng = engineers.get(payload["engineer_id"])
            desc = f"Allocation added: {eng.get('name') if eng else payload['engineer_id']} on {proj.get('name')}"
            add_changes(proj["client_id"], months_for_alloc(payload, proj), desc, mapping)
        elif ctype == "allocation_update":
            alloc_before = base_allocs.get(payload["allocation_id"])
            alloc_after = None
            for alloc in scenario["allocations"]:
                if alloc["id"] == payload["allocation_id"]:
                    alloc_after = alloc
                    break
            proj_before = base_projects.get(alloc_before["project_id"]) if alloc_before else None
            proj_after = projects.get(alloc_after["project_id"]) if alloc_after else None
            desc = f"Allocation updated: {payload['allocation_id']}"
            if alloc_before and proj_before:
                add_changes(proj_before["client_id"], months_for_alloc(alloc_before, proj_before), desc, mapping)
            if alloc_after and proj_after:
                add_changes(proj_after["client_id"], months_for_alloc(alloc_after, proj_after), desc, mapping)
        elif ctype == "allocation_delete":
            alloc_before = base_allocs.get(payload["allocation_id"])
            if not alloc_before:
                continue
            proj = base_projects.get(alloc_before["project_id"])
            if not proj:
                continue
            desc = f"Allocation deleted: {payload['allocation_id']}"
            add_changes(proj["client_id"], months_for_alloc(alloc_before, proj), desc, mapping)
        elif ctype == "project_add":
            desc = f"Project added: {payload['name']}"
            # allocations to this project will be handled by allocation_add
            # no direct impact otherwise
            continue
        elif ctype == "project_update":
            proj_id = payload["project_id"]
            proj = projects.get(proj_id)
            if not proj:
                continue
            desc = f"Project updated: {proj.get('name')}"
            for alloc in scenario["allocations"]:
                if alloc["project_id"] != proj_id:
                    continue
                add_changes(proj["client_id"], months_for_alloc(alloc, proj), desc, mapping)
        elif ctype == "project_delete":
            proj = base_projects.get(payload["project_id"])
            if not proj:
                continue
            desc = f"Project deleted: {proj.get('name')}"
            for alloc in base["allocations"]:
                if alloc["project_id"] != payload["project_id"]:
                    continue
                add_changes(proj["client_id"], months_for_alloc(alloc, proj), desc, mapping)
        elif ctype == "cell_adjust":
            desc = f"Manual cell adjustment: {payload.get('amount')}"
            add_changes(payload["client_id"], {payload["month"]}, desc, mapping)

    return mapping
