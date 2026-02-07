from __future__ import annotations

import curses
from dataclasses import dataclass
from pathlib import Path
from datetime import date, datetime
from typing import Callable, Iterable

from rich.console import Console
from rich.console import Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich import box

from .db import (
    DbError,
    connect,
    ensure_cohort,
    init_db,
    parse_date,
    validate_engineer_exists,
    validate_project_exists,
    validate_project_window,
)
from .revenue import client_revenue, engineer_revenue, project_revenue, client_revenue_year
from .scenarios import add_change, create_scenario, list_changes, list_scenarios, scenario_client_revenue_year
from .reports import projects_ending_with_details, unallocated_engineers


@dataclass
class Project:
    id: int
    name: str
    client: str
    start_date: str
    end_date: str | None
    status: str


@dataclass
class Engineer:
    id: int
    name: str
    level: int
    day_rate: float | None
    cohort: int
    active: int


@dataclass
class Client:
    id: int
    name: str


@dataclass
class FormField:
    key: str
    label: str
    value: str
    required: bool = False
    readonly: bool = False
    choices: list[str] | None = None


ADVANCED_MODE = False
COLOR_ENABLED = False
CONFIG_PATH = Path("~/.turntabl/config").expanduser()


def _load_config() -> None:
    global ADVANCED_MODE
    try:
        if not CONFIG_PATH.exists():
            return
        data = CONFIG_PATH.read_text(encoding="utf-8").strip()
        if data.lower().startswith("mode="):
            _, value = data.split("=", 1)
            ADVANCED_MODE = value.strip().lower() == "advanced"
    except OSError:
        pass


def _save_config() -> None:
    try:
        CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        mode = "advanced" if ADVANCED_MODE else "basic"
        CONFIG_PATH.write_text(f"mode={mode}\n", encoding="utf-8")
    except OSError:
        pass


def _fetch_projects(conn) -> list[Project]:
    cur = conn.execute(
        """
        SELECT p.id, p.name, c.name AS client, p.start_date, p.end_date, p.status
        FROM project p
        JOIN client c ON c.id = p.client_id
        ORDER BY p.start_date DESC, p.name ASC
        """
    )
    return [
        Project(
            id=row["id"],
            name=row["name"],
            client=row["client"],
            start_date=row["start_date"],
            end_date=row["end_date"],
            status=row["status"],
        )
        for row in cur.fetchall()
    ]


def _fetch_clients(conn) -> list[Client]:
    cur = conn.execute(
        """
        SELECT id, name
        FROM client
        ORDER BY name ASC
        """
    )
    return [Client(id=row["id"], name=row["name"]) for row in cur.fetchall()]


def _fetch_engineers(conn, name_like: str | None = None) -> list[Engineer]:
    if name_like:
        cur = conn.execute(
            """
            SELECT e.id, e.name, e.level, e.day_rate, e.active, c.number AS cohort
            FROM engineer e
            JOIN tlc_cohort c ON c.id = e.cohort_id
            WHERE e.name LIKE ?
            ORDER BY e.name ASC
            """,
            (f"%{name_like}%",),
        )
    else:
        cur = conn.execute(
            """
            SELECT e.id, e.name, e.level, e.day_rate, e.active, c.number AS cohort
            FROM engineer e
            JOIN tlc_cohort c ON c.id = e.cohort_id
            ORDER BY e.name ASC
            """
        )
    return [
        Engineer(
            id=row["id"],
            name=row["name"],
            level=row["level"],
            day_rate=row["day_rate"],
            cohort=row["cohort"],
            active=row["active"],
        )
        for row in cur.fetchall()
    ]


def _fuzzy_score(term: str, candidate: str) -> float:
    if not term:
        return 0.0
    term_l = term.lower()
    cand_l = candidate.lower()
    if term_l == cand_l:
        return 1000.0
    if cand_l.startswith(term_l):
        return 800.0 - (len(candidate) * 0.01)
    score = 0.0
    pos = 0
    for ch in term_l:
        idx = cand_l.find(ch, pos)
        if idx == -1:
            return 0.0
        # reward contiguous matches
        score += 5.0 if idx == pos else 1.0
        pos = idx + 1
    # shorter candidate is slightly better
    score -= len(candidate) * 0.01
    return score


def _fuzzy_sort(term: str, items: list[tuple[str, object]]) -> list[object]:
    scored = []
    for label, obj in items:
        score = _fuzzy_score(term, label)
        if score > 0:
            scored.append((score, label, obj))
    scored.sort(key=lambda t: (-t[0], t[1].lower()))
    return [obj for _, _, obj in scored]


def _fuzzy_sort_with_labels(term: str, items: list[tuple[str, object]]) -> list[tuple[str, object]]:
    scored = []
    for label, obj in items:
        score = _fuzzy_score(term, label)
        if score > 0:
            scored.append((score, label, obj))
    scored.sort(key=lambda t: (-t[0], t[1].lower()))
    return [(label, obj) for _, label, obj in scored]


def _fetch_project_allocations(conn, project_id: int) -> list[dict]:
    cur = conn.execute(
        """
        SELECT a.id, a.start_date, a.end_date, a.status,
               e.id AS engineer_id, e.name AS engineer
        FROM allocation a
        JOIN engineer e ON e.id = a.engineer_id
        WHERE a.project_id = ?
        ORDER BY a.start_date ASC
        """,
        (project_id,),
    )
    return [dict(row) for row in cur.fetchall()]


def _fetch_engineer_allocations(conn, engineer_id: int) -> list[dict]:
    cur = conn.execute(
        """
        SELECT a.id, a.start_date, a.end_date, a.status,
               p.id AS project_id, p.name AS project,
               c.name AS client
        FROM allocation a
        JOIN project p ON p.id = a.project_id
        JOIN client c ON c.id = p.client_id
        WHERE a.engineer_id = ?
        ORDER BY a.start_date ASC
        """,
        (engineer_id,),
    )
    return [dict(row) for row in cur.fetchall()]


def _fetch_allocation_detail(conn, allocation_id: int) -> dict | None:
    cur = conn.execute(
        """
        SELECT
            a.id AS allocation_id,
            a.start_date,
            a.end_date,
            a.status AS allocation_status,
            e.id AS engineer_id,
            e.name AS engineer,
            e.day_rate,
            p.id AS project_id,
            p.name AS project,
            p.agreed_rate,
            p.status AS project_status,
            c.name AS client
        FROM allocation a
        JOIN engineer e ON e.id = a.engineer_id
        JOIN project p ON p.id = a.project_id
        JOIN client c ON c.id = p.client_id
        WHERE a.id = ?
        """,
        (allocation_id,),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def _parse_iso(value: str) -> date:
    return date.fromisoformat(value)


def _prompt(stdscr, prompt: str) -> str:
    curses.echo()
    stdscr.addstr(prompt)
    stdscr.clrtoeol()
    stdscr.refresh()
    value = stdscr.getstr().decode("utf-8").strip()
    curses.noecho()
    return value


def _status_label(status: str) -> str:
    return "Provisional" if status == "provisional" else "Confirmed"


def _cycle_choice(field: FormField, direction: int) -> None:
    if not field.choices:
        return
    if field.value not in field.choices:
        field.value = field.choices[0]
        return
    idx = field.choices.index(field.value)
    field.value = field.choices[(idx + direction) % len(field.choices)]


def _edit_field_value(stdscr, row: int, col: int, current: str, max_width: int) -> str:
    curses.echo()
    stdscr.move(row, col)
    stdscr.clrtoeol()
    stdscr.addstr(row, col, current[: max(0, max_width - 1)])
    stdscr.refresh()
    value = stdscr.getstr().decode("utf-8").strip()
    curses.noecho()
    return value


def _form_screen(
    stdscr,
    title: str,
    subtitle: str | None,
    fields: list[FormField],
) -> dict | None:
    selected = 0
    while True:
        start_row = _draw_header(stdscr, title, subtitle)
        _draw_hints(
            stdscr,
            start_row,
            [("↑/↓", "move"), ("Enter", "edit"), ("←/→", "toggle"), ("s", "save"), ("b", "back")],
        )
        start_row += 1
        height, width = stdscr.getmaxyx()
        max_rows = max(1, height - start_row - 1)
        offset = 0
        if selected >= max_rows:
            offset = selected - max_rows + 1
        view = fields[offset : offset + max_rows]
        for idx, field in enumerate(view):
            absolute = offset + idx
            value = field.value
            if field.choices and field.value in field.choices:
                value = _status_label(field.value) if field.key.endswith("_status") else field.value
            line = f"{field.label}: {value}"
            if field.required and not field.value:
                line += " *"
            attr = curses.A_REVERSE if absolute == selected else 0
            stdscr.addstr(start_row + idx, 0, line[: width - 1], attr)
        stdscr.refresh()
        key = stdscr.getch()
        if key in (27, ord("b")):
            return None
        if key in (curses.KEY_UP, ord("k")):
            selected = max(0, selected - 1)
            continue
        if key in (curses.KEY_DOWN, ord("j")):
            selected = min(len(fields) - 1, selected + 1)
            continue
        current = fields[selected]
        if key in (curses.KEY_LEFT, ord("h")):
            if not current.readonly and current.choices:
                _cycle_choice(current, -1)
            continue
        if key in (curses.KEY_RIGHT, ord("l")):
            if not current.readonly and current.choices:
                _cycle_choice(current, 1)
            continue
        if key in (curses.KEY_ENTER, 10, 13):
            if current.readonly:
                continue
            if current.choices:
                _cycle_choice(current, 1)
                continue
            row = start_row + (selected - offset)
            col = len(f"{current.label}: ")
            current.value = _edit_field_value(stdscr, row, col, current.value, width - col)
            continue
        if key in (ord("s"),):
            missing = [f.label for f in fields if f.required and not f.value]
            if missing:
                _draw_header(stdscr, "Error", f"Missing: {', '.join(missing)}")
                stdscr.refresh()
                stdscr.getch()
                continue
            return {f.key: f.value for f in fields}


def _select_with_search(
    stdscr,
    title: str,
    subtitle: str,
    items: list[tuple[str, object]],
    search_prompt: str,
) -> object | None:
    del subtitle
    prompt_label = search_prompt.rstrip(": ")
    return _fuzzy_modal_select(stdscr, title, prompt_label, items)


def _render_rich_lines(renderable, width: int) -> list[str]:
    console = Console(record=True, width=max(20, width), force_terminal=False, color_system=None)
    console.print(renderable)
    return console.export_text(styles=False).splitlines()


def _fuzzy_modal_select(
    stdscr,
    title: str,
    prompt_label: str,
    items: list[tuple[str, object]],
) -> object | None:
    term = ""
    selected = 0
    offset = 0
    while True:
        height, width = stdscr.getmaxyx()
        modal_height = max(10, min(height - 2, 20))
        modal_width = max(40, min(width - 4, 90))
        start_y = (height - modal_height) // 2
        start_x = (width - modal_width) // 2

        results = items if not term else _fuzzy_sort_with_labels(term, items)
        visible_rows = max(1, modal_height - 6)
        selected = min(selected, max(0, len(results) - 1))
        if selected < offset:
            offset = selected
        if selected >= offset + visible_rows:
            offset = selected - visible_rows + 1
        window_items = results[offset : offset + visible_rows]

        lines = [Text(f"{prompt_label}: {term}", style="bold")]
        if not results:
            lines.append(Text("(no results)", style="dim"))
        for idx, (label, _) in enumerate(window_items):
            absolute = offset + idx
            prefix = "▶ " if absolute == selected else "  "
            style = "reverse" if absolute == selected else ""
            lines.append(Text(prefix + label, style=style))

        panel = Panel(
            Group(*lines),
            title=title,
            subtitle="type to search • ↑/↓ move • Enter select • Esc cancel",
            border_style="cyan",
            box=box.ROUNDED,
            width=modal_width,
        )
        rendered = _render_rich_lines(panel, modal_width)
        stdscr.clear()
        for idx, line in enumerate(rendered[:modal_height]):
            stdscr.addstr(start_y + idx, start_x, line[: modal_width - 1])
        stdscr.refresh()

        key = stdscr.getch()
        if key in (27, ord("q"), ord("b")):
            return None
        if key in (curses.KEY_ENTER, 10, 13):
            if results:
                return results[selected][1]
            continue
        if key in (curses.KEY_UP, ord("k")):
            selected = max(0, selected - 1)
            continue
        if key in (curses.KEY_DOWN, ord("j")):
            selected = min(max(0, len(results) - 1), selected + 1)
            continue
        if key in (curses.KEY_BACKSPACE, 127, 8):
            term = term[:-1]
            selected = 0
            offset = 0
            continue
        if 32 <= key <= 126:
            term += chr(key)
            selected = 0
            offset = 0


def _draw_header(stdscr, title: str, subtitle: str | None = None) -> int:
    stdscr.clear()
    _, width = stdscr.getmaxyx()
    content = Text(title, style="bold cyan")
    if subtitle:
        content.append("\n")
        content.append(subtitle)
    panel = Panel(content, box=box.ROUNDED, border_style="cyan")
    rendered = _render_rich_lines(panel, width - 1)
    for idx, line in enumerate(rendered):
        stdscr.addstr(idx, 0, line[: width - 1])
    return len(rendered)


def _draw_hints(stdscr, row: int, hints: list[tuple[str, str]]) -> None:
    del row
    height, width = stdscr.getmaxyx()
    if ADVANCED_MODE:
        hint_text = " ".join(key for key, _ in hints)
    else:
        hint_text = " • ".join(f"[{key}] {word}" for key, word in hints)
    text = hint_text[: max(0, width - 1)]
    try:
        stdscr.addstr(height - 1, 0, text.ljust(max(0, width - 1)), curses.A_REVERSE)
    except curses.error:
        pass


def _allocation_detail_screen(stdscr, conn, allocation_id: int) -> None:
    detail = _fetch_allocation_detail(conn, allocation_id)
    if not detail:
        _draw_header(stdscr, "Allocation", "Not found")
        stdscr.refresh()
        stdscr.getch()
        return
    start = detail["start_date"]
    end = detail["end_date"]
    end_label = end if end else "open"
    day_rate = detail["day_rate"]
    agreed_rate = detail["agreed_rate"]
    effective_rate = agreed_rate if agreed_rate is not None else day_rate
    total_days = None
    total_cost = None
    if end:
        total_days = (_parse_iso(end) - _parse_iso(start)).days + 1
        if effective_rate is not None:
            total_cost = total_days * effective_rate

    lines = [
        f"Allocation: {detail['allocation_id']}",
        f"Engineer: {detail['engineer']} (id {detail['engineer_id']})",
        f"Project: {detail['project']} (id {detail['project_id']})",
        f"Client: {detail['client']}",
        f"Allocation status: {detail.get('allocation_status', 'confirmed')}",
        f"Project status: {detail.get('project_status', 'confirmed')}",
        f"Dates: {start} -> {end_label}",
        f"Engineer day rate: {day_rate if day_rate is not None else 'n/a'}",
        f"Project agreed rate: {agreed_rate if agreed_rate is not None else 'n/a'}",
        f"Effective rate: {effective_rate if effective_rate is not None else 'n/a'}",
        f"Total days: {total_days if total_days is not None else 'n/a'}",
        f"Total cost: {total_cost if total_cost is not None else 'n/a'}",
    ]
    while True:
        start_row = _draw_header(stdscr, "Allocation Detail", None)
        _draw_hints(stdscr, start_row, [("b", "back")])
        start_row += 1
        height, width = stdscr.getmaxyx()
        for idx, line in enumerate(lines[: max(1, height - start_row - 1)]):
            stdscr.addstr(start_row + idx, 0, line[: width - 1])
        stdscr.refresh()
        key = stdscr.getch()
        if key in (27, ord("b")):
            return


def _draw_list(stdscr, items: list[str], selected: int, start_row: int) -> None:
    height, width = stdscr.getmaxyx()
    max_rows = max(1, height - start_row - 1)
    offset = 0
    if selected >= max_rows:
        offset = selected - max_rows + 1
    view = items[offset : offset + max_rows]
    rows = []
    for idx, item in enumerate(view):
        absolute = offset + idx
        prefix = "▶ " if absolute == selected else "  "
        style = "reverse" if absolute == selected else ""
        rows.append(Text(prefix + item, style=style))
    rendered = _render_rich_lines(Panel(Group(*rows), box=box.SIMPLE), width - 1)
    for idx, line in enumerate(rendered[: max_rows]):
        stdscr.addstr(start_row + idx, 0, line[: width - 1])


def _select_from_list(
    stdscr,
    title: str,
    subtitle: str,
    items: list[str],
) -> int | None:
    if not items:
        _draw_header(stdscr, title, "(no results)")
        stdscr.refresh()
        stdscr.getch()
        return None
    selected = 0
    while True:
        start_row = _draw_header(stdscr, title, subtitle)
        _draw_list(stdscr, items, selected, start_row)
        stdscr.refresh()
        key = stdscr.getch()
        if key in (curses.KEY_UP, ord("k")):
            selected = max(0, selected - 1)
        elif key in (curses.KEY_DOWN, ord("j")):
            selected = min(len(items) - 1, selected + 1)
        elif key in (curses.KEY_ENTER, 10, 13):
            return selected
        elif key in (27, ord("q")):
            return None


def _gantt_rows(allocations: list[dict], start: date, end: date, width: int) -> list[str]:
    total_days = (end - start).days + 1
    if total_days <= 0:
        return []
    scale = max(1, total_days // max(1, width - 20))
    rows = []
    for alloc in allocations:
        a_start = _parse_iso(alloc["start_date"])
        a_end = _parse_iso(alloc["end_date"]) if alloc["end_date"] else end
        start_offset = max(0, (a_start - start).days) // scale
        end_offset = max(0, (a_end - start).days) // scale
        bar = [" "] * max(1, (total_days // scale) + 1)
        mark = "." if alloc.get("status") == "provisional" else "#"
        for i in range(start_offset, min(end_offset + 1, len(bar))):
            bar[i] = mark
        label = f"{alloc['engineer']:<16}"[:16]
        rows.append(label + " " + "".join(bar))
    return rows


def _render_engineer_gaps(allocations: list[dict]) -> list[str]:
    if not allocations:
        return ["(no allocations)"]
    gaps = []
    last_end = None
    for alloc in allocations:
        start = _parse_iso(alloc["start_date"])
        if alloc["end_date"] is None:
            last_end = date.today()
            break
        end = _parse_iso(alloc["end_date"])
        if last_end and start > last_end:
            gaps.append(f"Gap: {last_end.isoformat()} -> {start.isoformat()}")
        last_end = max(last_end, end) if last_end else end
    if not gaps:
        gaps.append("No gaps")
    return gaps


def _add_allocation_prompt(conn, stdscr, engineer_id: int, project_id: int) -> str | None:
    try:
        e_row = conn.execute("SELECT name FROM engineer WHERE id = ?", (engineer_id,)).fetchone()
        p_row = conn.execute(
            "SELECT name, status, end_date FROM project WHERE id = ?", (project_id,)
        ).fetchone()
        engineer_label = e_row["name"] if e_row else f"id {engineer_id}"
        project_label = p_row["name"] if p_row else f"id {project_id}"
        default_status = p_row["status"] if p_row and p_row["status"] else "confirmed"
        fields = [
            FormField("engineer", "Engineer", engineer_label, readonly=True),
            FormField("project", "Project", project_label, readonly=True),
            FormField("start_date", "Start date (YYYY-MM-DD)", "", required=True),
            FormField("end_date", "End date (YYYY-MM-DD, blank=open)", ""),
            FormField("allocation_status", "Status", default_status, choices=["confirmed", "provisional"]),
        ]
        values = _form_screen(stdscr, "Add Allocation", None, fields)
        if values is None:
            return "Allocation creation cancelled."
        start_iso = parse_date(values["start_date"])
        end_iso = None
        if values["end_date"]:
            end_iso = parse_date(values["end_date"])
            if start_iso > end_iso:
                return "Start date must be on or before end date."
        validate_engineer_exists(conn, engineer_id)
        validate_project_exists(conn, project_id)
        if end_iso is None:
            cur = conn.execute("SELECT end_date FROM project WHERE id = ?", (project_id,))
            row = cur.fetchone()
            if row and row["end_date"] is not None:
                return "Open-ended allocations require an open-ended project."
            validate_project_window(conn, project_id, start_iso, start_iso)
        else:
            validate_project_window(conn, project_id, start_iso, end_iso)
        conn.execute(
            """
            INSERT INTO allocation (engineer_id, project_id, start_date, end_date, status)
            VALUES (?, ?, ?, ?, ?)
            """,
            (engineer_id, project_id, start_iso, end_iso, values["allocation_status"]),
        )
        conn.commit()
        return None
    except DbError as exc:
        return str(exc)


def _add_project_prompt(conn, stdscr) -> str | None:
    try:
        clients = _fetch_clients(conn)
        client_items = [(c.name, c) for c in clients]
        client = _select_with_search(
            stdscr,
            "Select client",
            "Type to search, Enter to select, q to cancel",
            client_items,
            "Client search: ",
        )
        if client is None:
            return "Client selection cancelled."
        fields = [
            FormField("name", "Project name", "", required=True),
            FormField("start_date", "Start date (YYYY-MM-DD)", "", required=True),
            FormField("end_date", "End date (YYYY-MM-DD, blank=open)", ""),
            FormField("agreed_rate", "Agreed rate (blank for none)", ""),
            FormField("project_status", "Status", "confirmed", choices=["confirmed", "provisional"]),
        ]
        values = _form_screen(stdscr, "Add Project", f"Client: {client.name}", fields)
        if values is None:
            return "Project creation cancelled."
        start_iso = parse_date(values["start_date"])
        end_iso = None
        if values["end_date"]:
            end_iso = parse_date(values["end_date"])
            if start_iso > end_iso:
                return "Project start date must be on or before end date."
        agreed_rate = float(values["agreed_rate"]) if values["agreed_rate"] else None
        status = values["project_status"]
        conn.execute(
            """
            INSERT INTO project (client_id, name, start_date, end_date, agreed_rate, status)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (client.id, values["name"], start_iso, end_iso, agreed_rate, status),
        )
        conn.commit()
        return None
    except (ValueError, DbError) as exc:
        return str(exc)


def _add_engineer_prompt(conn, stdscr) -> str | None:
    try:
        fields = [
            FormField("name", "Engineer name", "", required=True),
            FormField("level", "Level (1-5)", "", required=True),
            FormField("cohort", "Cohort (1-8)", "", required=True),
            FormField("day_rate", "Day rate (blank for none)", ""),
            FormField("active", "Active", "yes", choices=["yes", "no"]),
        ]
        values = _form_screen(stdscr, "Add Engineer", None, fields)
        if values is None:
            return "Engineer creation cancelled."
        level = int(values["level"])
        cohort = int(values["cohort"])
        day_rate = float(values["day_rate"]) if values["day_rate"] else None
        active = 1 if values["active"] == "yes" else 0
        cohort_id = ensure_cohort(conn, cohort)
        conn.execute(
            "INSERT INTO engineer (name, level, day_rate, cohort_id, active) VALUES (?, ?, ?, ?, ?)",
            (values["name"], level, day_rate, cohort_id, active),
        )
        conn.commit()
        return None
    except (ValueError, DbError) as exc:
        return str(exc)


def _add_client_prompt(conn, stdscr) -> str | None:
    try:
        fields = [FormField("name", "Client name", "", required=True)]
        values = _form_screen(stdscr, "Add Client", None, fields)
        if values is None:
            return "Client creation cancelled."
        if not values["name"]:
            return "Client name is required."
        conn.execute("INSERT INTO client (name) VALUES (?)", (values["name"],))
        conn.commit()
        return None
    except (ValueError, DbError) as exc:
        return str(exc)

def _project_screen(stdscr, conn, project: Project):
    selected = 0
    while True:
        allocs = _fetch_project_allocations(conn, project.id)
        rev_rows = project_revenue(conn, date.today())
        rev = next((r for r in rev_rows if r["project_id"] == project.id), None)
        start_row = _draw_header(
            stdscr,
            f"Project: {project.name} (Client: {project.client})",
            f"{project.start_date} -> {project.end_date or 'open'}",
        )
        _draw_hints(stdscr, start_row, [("o", "open"), ("a", "add"), ("g", "gantt"), ("b", "back")])
        start_row += 1
        if rev:
            stdscr.addstr(
                start_row,
                0,
                f"Revenue to date: {rev['revenue_to_date']:.2f} | Total: {rev['revenue_total']:.2f}",
            )
            start_row += 1
        lines = []
        for a in allocs:
            end_label = a["end_date"] if a["end_date"] else "open"
            status = " (prov)" if a.get("status") == "provisional" else ""
            lines.append(
                f"{a['engineer']} | {a['start_date']} -> {end_label} (alloc {a['id']}){status}"
            )
        if not lines:
            lines = ["(no allocations)"]
        _draw_list(stdscr, lines, selected, start_row)
        stdscr.refresh()
        key = stdscr.getch()
        if key in (27, ord("b")):
            return
        if key in (curses.KEY_UP, ord("k")):
            selected = max(0, selected - 1)
        if key in (curses.KEY_DOWN, ord("j")):
            selected = min(len(lines) - 1, selected + 1)
        if key in (curses.KEY_ENTER, 10, 13, ord("o")):
            if allocs:
                _allocation_detail_screen(stdscr, conn, allocs[selected]["id"])
        if key == ord("a"):
            engineers = _fetch_engineers(conn)
            items = [(f"{e.name} (id {e.id})", e) for e in engineers]
            engineer = _select_with_search(
                stdscr,
                "Select engineer",
                "Type to search, Enter to select, q to cancel",
                items,
                "Engineer search: ",
            )
            if engineer is None:
                continue
            err = _add_allocation_prompt(conn, stdscr, engineer.id, project.id)
            if err:
                _draw_header(stdscr, "Error", err)
                stdscr.refresh()
                stdscr.getch()
        if key == ord("g"):
            height, width = stdscr.getmaxyx()
            if project.end_date:
                gantt_end = _parse_iso(project.end_date)
            else:
                # extend to max allocation end, or 90 days from today if none
                max_end = None
                for a in allocs:
                    if a["end_date"]:
                        dt = _parse_iso(a["end_date"])
                    else:
                        dt = date.today()
                    max_end = dt if max_end is None or dt > max_end else max_end
                if max_end is None:
                    max_end = date.today()
                gantt_end = max_end.fromordinal(max_end.toordinal() + 90)
            gantt = _gantt_rows(allocs, _parse_iso(project.start_date), gantt_end, width)
            offset = 0
            while True:
                start_row = _draw_header(
                    stdscr,
                    f"Project Gantt: {project.name}",
                    f"{project.start_date} -> {project.end_date or 'open'}",
                )
                _draw_hints(stdscr, start_row, [("j", "down"), ("k", "up"), ("b", "back")])
                start_row += 1
                visible = max(1, height - start_row - 1)
                view = gantt[offset : offset + visible]
                for idx, line in enumerate(view):
                    stdscr.addstr(start_row + idx, 0, line[: width - 1])
                stdscr.refresh()
                key2 = stdscr.getch()
                if key2 in (27, ord("b")):
                    break
                if key2 in (curses.KEY_DOWN, ord("j")):
                    offset = min(max(0, len(gantt) - visible), offset + 1)
                if key2 in (curses.KEY_UP, ord("k")):
                    offset = max(0, offset - 1)


def _engineer_screen(stdscr, conn, engineer: Engineer):
    selected = 0
    while True:
        allocs = _fetch_engineer_allocations(conn, engineer.id)
        rev_rows = engineer_revenue(conn, date.today())
        rev = next((r for r in rev_rows if r["engineer_id"] == engineer.id), None)
        start_row = _draw_header(
            stdscr,
            f"Engineer: {engineer.name} (Level {engineer.level}, Cohort {engineer.cohort})",
            None,
        )
        _draw_hints(stdscr, start_row, [("o", "open"), ("a", "add"), ("b", "back")])
        start_row += 1
        if rev:
            stdscr.addstr(
                start_row,
                0,
                f"Revenue to date: {rev['revenue_to_date']:.2f} | Total: {rev['revenue_total']:.2f}",
            )
            start_row += 1
        lines = []
        for a in allocs:
            end_label = a["end_date"] if a["end_date"] else "open"
            status = " (prov)" if a.get("status") == "provisional" else ""
            lines.append(
                f"{a['project']} ({a['client']}) | {a['start_date']} -> {end_label} (alloc {a['id']}){status}"
            )
        alloc_count = len(allocs)
        if alloc_count == 0:
            selected = 0
        else:
            selected = min(selected, alloc_count - 1)
        if not lines:
            lines = ["(no allocations)"]
        lines.extend([""])
        lines.append("Work gaps:")
        lines.extend(_render_engineer_gaps(allocs))
        _draw_list(stdscr, lines, selected, start_row)
        stdscr.refresh()
        key = stdscr.getch()
        if key in (27, ord("b")):
            return
        if key in (curses.KEY_UP, ord("k")):
            selected = max(0, selected - 1)
        if key in (curses.KEY_DOWN, ord("j")):
            selected = min(max(0, alloc_count - 1), selected + 1) if alloc_count else 0
        if key == ord("a"):
            projects = _fetch_projects(conn)
            items = [(f"{p.name} (id {p.id})", p) for p in projects]
            project = _select_with_search(
                stdscr,
                "Select project",
                "Type to search, Enter to select, q to cancel",
                items,
                "Project search: ",
            )
            if project is None:
                continue
            err = _add_allocation_prompt(conn, stdscr, engineer.id, project.id)
            if err:
                _draw_header(stdscr, "Error", err)
                stdscr.refresh()
                stdscr.getch()
        if key in (curses.KEY_ENTER, 10, 13, ord("o")):
            if alloc_count:
                _allocation_detail_screen(stdscr, conn, allocs[selected]["id"])


def _projects_list(stdscr, conn):
    projects = _fetch_projects(conn)
    selected = 0
    while True:
        if projects:
            items = [
                f"{p.name} ({p.client}) {p.start_date} -> {p.end_date or 'open'}"
                for p in projects
            ]
        else:
            items = ["(no projects yet)"]
        start_row = _draw_header(stdscr, "Projects", None)
        _draw_hints(stdscr, start_row, [("o", "open"), ("/", "search"), ("a", "add"), ("b", "back")])
        start_row += 1
        _draw_list(stdscr, items, selected, start_row)
        stdscr.refresh()
        key = stdscr.getch()
        if key in (curses.KEY_UP, ord("k")):
            selected = max(0, selected - 1)
        elif key in (curses.KEY_DOWN, ord("j")):
            selected = min(len(projects) - 1, selected + 1)
        elif key in (ord("/"), ord("s")):
            all_projects = _fetch_projects(conn)
            items_pairs = [
                (f"{p.name} ({p.client}) {p.start_date} -> {p.end_date or 'open'}", p)
                for p in all_projects
            ]
            choice = _select_with_search(stdscr, "Search projects", "", items_pairs, "Search")
            if choice:
                projects = all_projects
                selected = next((idx for idx, p in enumerate(projects) if p.id == choice.id), 0)
        elif key == ord("a"):
            err = _add_project_prompt(conn, stdscr)
            if err:
                _draw_header(stdscr, "Error", err)
                stdscr.refresh()
                stdscr.getch()
            projects = _fetch_projects(conn)
            selected = min(selected, max(0, len(projects) - 1))
        elif key in (curses.KEY_ENTER, 10, 13, ord("o")):
            if projects:
                _project_screen(stdscr, conn, projects[selected])
                projects = _fetch_projects(conn)
                selected = min(selected, max(0, len(projects) - 1))
        elif key in (27, ord("b")):
            return


def _engineers_list(stdscr, conn):
    engineers = _fetch_engineers(conn)
    selected = 0
    while True:
        if engineers:
            items = [f"{e.name} (L{e.level}, Cohort {e.cohort})" for e in engineers]
        else:
            items = ["(no engineers yet)"]
        start_row = _draw_header(stdscr, "Engineers", None)
        _draw_hints(stdscr, start_row, [("o", "open"), ("/", "search"), ("a", "add"), ("b", "back")])
        start_row += 1
        _draw_list(stdscr, items, selected, start_row)
        stdscr.refresh()
        key = stdscr.getch()
        if key in (curses.KEY_UP, ord("k")):
            selected = max(0, selected - 1)
        elif key in (curses.KEY_DOWN, ord("j")):
            selected = min(len(engineers) - 1, selected + 1)
        elif key in (ord("/"), ord("s")):
            all_engineers = _fetch_engineers(conn)
            items = [(f"{e.name} (L{e.level}, Cohort {e.cohort})", e) for e in all_engineers]
            choice = _select_with_search(stdscr, "Search engineers", "", items, "Search")
            if choice:
                engineers = all_engineers
                selected = next((idx for idx, e in enumerate(engineers) if e.id == choice.id), 0)
        elif key == ord("a"):
            err = _add_engineer_prompt(conn, stdscr)
            if err:
                _draw_header(stdscr, "Error", err)
                stdscr.refresh()
                stdscr.getch()
            engineers = _fetch_engineers(conn)
            selected = min(selected, max(0, len(engineers) - 1))
        elif key in (curses.KEY_ENTER, 10, 13, ord("o")):
            if engineers:
                _engineer_screen(stdscr, conn, engineers[selected])
                engineers = _fetch_engineers(conn)
                selected = min(selected, max(0, len(engineers) - 1))
        elif key in (27, ord("b")):
            return


def _clients_list(stdscr, conn):
    clients = _fetch_clients(conn)
    selected = 0
    while True:
        rev_rows = client_revenue(conn, date.today())
        rev_map = {r["client_id"]: r for r in rev_rows}
        if clients:
            items = []
            for c in clients:
                rev = rev_map.get(c.id)
                if rev:
                    items.append(
                        f"{c.name} (id {c.id}) | {rev['revenue_to_date']:.2f} | {rev['revenue_total']:.2f}"
                    )
                else:
                    items.append(f"{c.name} (id {c.id}) | 0.00 | 0.00")
        else:
            items = ["(no clients yet)"]
        start_row = _draw_header(stdscr, "Clients", None)
        _draw_hints(stdscr, start_row, [("/", "search"), ("a", "add"), ("b", "back")])
        start_row += 1
        _draw_list(stdscr, items, selected, start_row)
        stdscr.refresh()
        key = stdscr.getch()
        if key in (curses.KEY_UP, ord("k")):
            selected = max(0, selected - 1)
        elif key in (curses.KEY_DOWN, ord("j")):
            selected = min(len(clients) - 1, selected + 1)
        elif key in (ord("/"), ord("s")):
            all_clients = _fetch_clients(conn)
            items_pairs = [(c.name, c) for c in all_clients]
            choice = _select_with_search(stdscr, "Search clients", "", items_pairs, "Search")
            if choice:
                clients = all_clients
                selected = next((idx for idx, c in enumerate(clients) if c.id == choice.id), 0)
        elif key == ord("a"):
            err = _add_client_prompt(conn, stdscr)
            if err:
                _draw_header(stdscr, "Error", err)
                stdscr.refresh()
                stdscr.getch()
            clients = _fetch_clients(conn)
            selected = min(selected, max(0, len(clients) - 1))
        elif key in (27, ord("b")):
            return


def _config_screen(stdscr) -> None:
    global ADVANCED_MODE
    selected = 0 if not ADVANCED_MODE else 1
    while True:
        start_row = _draw_header(stdscr, "Config", None)
        _draw_hints(
            stdscr,
            start_row,
            [("←/→", "select"), ("Enter", "apply"), ("s", "basic"), ("a", "advanced"), ("b", "back")],
        )
        start_row += 1
        label_basic = "[Basic]" if selected == 0 else "Basic"
        label_adv = "[Advanced]" if selected == 1 else "Advanced"
        line = f"Menus: {label_basic} {label_adv}"
        _, width = stdscr.getmaxyx()
        stdscr.addstr(start_row, 0, line[: width - 1])
        stdscr.refresh()
        key = stdscr.getch()
        if key in (curses.KEY_LEFT, ord("h")):
            selected = 0
        elif key in (curses.KEY_RIGHT, ord("l")):
            selected = 1
        elif key in (curses.KEY_ENTER, 10, 13):
            ADVANCED_MODE = selected == 1
            _save_config()
        elif key in (27, ord("b")):
            return
        elif key in (ord("s"),):
            selected = 0
            ADVANCED_MODE = False
            _save_config()
        elif key in (ord("a"),):
            selected = 1
            ADVANCED_MODE = True
            _save_config()


def _reports_screen(stdscr, conn) -> None:
    options = [
        "Project Revenue",
        "Client Revenue",
        "Engineer Revenue",
        "Client Revenue Year",
        "Scenario Revenue Year",
        "Engineer Gantt",
        "Unallocated Engineer Gantt",
        "Projects Ending Soon",
        "Back",
    ]
    selected = 0
    include_provisional = True
    while True:
        subtitle = f"Include provisional: {'Yes' if include_provisional else 'No'} (t toggle)"
        start_row = _draw_header(stdscr, "Reports", subtitle)
        _draw_hints(
            stdscr,
            start_row,
            [
                ("p", "project"),
                ("c", "client"),
                ("e", "engineer"),
                ("y", "year"),
                ("s", "scenario"),
                ("g", "gantt"),
                ("u", "unalloc"),
                ("n", "ending"),
                ("t", "toggle provisional"),
                ("b", "back"),
            ],
        )
        start_row += 1
        _draw_list(stdscr, options, selected, start_row)
        stdscr.refresh()
        key = stdscr.getch()
        if key in (curses.KEY_UP, ord("k")):
            selected = max(0, selected - 1)
        elif key in (curses.KEY_DOWN, ord("j")):
            selected = min(len(options) - 1, selected + 1)
        elif key in (ord("t"),):
            include_provisional = not include_provisional
        elif key in (27, ord("b")):
            return
        elif key in (curses.KEY_ENTER, 10, 13):
            choice = options[selected]
            if choice == "Project Revenue":
                _report_table(
                    stdscr,
                    "Project Revenue",
                    project_revenue(conn, date.today(), include_provisional),
                )
            elif choice == "Client Revenue":
                _report_table(
                    stdscr,
                    "Client Revenue",
                    client_revenue(conn, date.today(), include_provisional),
                )
            elif choice == "Engineer Revenue":
                _report_table(
                    stdscr,
                    "Engineer Revenue",
                    engineer_revenue(conn, date.today(), include_provisional),
                )
            elif choice == "Client Revenue Year":
                year = _prompt(stdscr, "Year (YYYY): ")
                year_val = int(year) if year else date.today().year
                _report_table(
                    stdscr,
                    f"Client Revenue {year_val}",
                    client_revenue_year(conn, year_val, include_provisional),
                )
            elif choice == "Scenario Revenue Year":
                _scenario_list_screen(stdscr, conn)
            elif choice == "Engineer Gantt":
                _engineer_gantt_report(stdscr, conn, False, include_provisional)
            elif choice == "Unallocated Engineer Gantt":
                _engineer_gantt_report(stdscr, conn, True, include_provisional)
            elif choice == "Projects Ending Soon":
                _projects_ending_report(stdscr, conn, include_provisional)
            else:
                return
        elif key == ord("p"):
            _report_table(
                stdscr,
                "Project Revenue",
                project_revenue(conn, date.today(), include_provisional),
            )
        elif key == ord("c"):
            _report_table(
                stdscr,
                "Client Revenue",
                client_revenue(conn, date.today(), include_provisional),
            )
        elif key == ord("e"):
            _report_table(
                stdscr,
                "Engineer Revenue",
                engineer_revenue(conn, date.today(), include_provisional),
            )
        elif key == ord("y"):
            year = _prompt(stdscr, "Year (YYYY): ")
            year_val = int(year) if year else date.today().year
            _report_table(
                stdscr,
                f"Client Revenue {year_val}",
                client_revenue_year(conn, year_val, include_provisional),
            )
        elif key == ord("s"):
            _scenario_list_screen(stdscr, conn)
        elif key == ord("g"):
            _engineer_gantt_report(stdscr, conn, False, include_provisional)
        elif key == ord("u"):
            _engineer_gantt_report(stdscr, conn, True, include_provisional)
        elif key == ord("n"):
            _projects_ending_report(stdscr, conn, include_provisional)


def _report_table(stdscr, title: str, rows: list[dict]) -> None:
    if not rows:
        _draw_header(stdscr, title, "(no data)")
        stdscr.refresh()
        stdscr.getch()
        return

    headers = list(rows[0].keys())
    table = Table(title=title, show_lines=False)
    col_widths = {h: len(h) for h in headers}
    for row in rows:
        for h in headers:
            val = row[h]
            if isinstance(val, float):
                val = f"{val:.2f}"
            col_widths[h] = max(col_widths[h], len(str(val)))
    for h in headers:
        justify = "right" if isinstance(rows[0][h], (int, float)) else "left"
        table.add_column(h, justify=justify, no_wrap=True)
    for row in rows:
        table.add_row(*[str(row[h]) for h in headers])

    # Estimate required width so Rich doesn't wrap
    width_est = sum(col_widths[h] for h in headers) + (3 * (len(headers) - 1)) + 2
    console = Console(record=True, width=max(width_est, stdscr.getmaxyx()[1] - 1))
    console.print(table)
    rendered = console.export_text().splitlines()

    offset = 0
    h_offset = 0
    while True:
        start_row = _draw_header(stdscr, title, None)
        _draw_hints(stdscr, start_row, [("j", "down"), ("k", "up"), ("h", "left"), ("l", "right"), ("b", "back")])
        start_row += 1
        height, width = stdscr.getmaxyx()
        visible = max(1, height - start_row - 1)
        view = rendered[offset : offset + visible]
        for idx, line in enumerate(view):
            clipped = line[h_offset : h_offset + width - 1]
            stdscr.addstr(start_row + idx, 0, clipped)
        stdscr.refresh()
        key = stdscr.getch()
        if key in (27, ord("b")):
            return
        if key in (curses.KEY_DOWN, ord("j")):
            offset = min(max(0, len(rendered) - visible), offset + 1)
        if key in (curses.KEY_UP, ord("k")):
            offset = max(0, offset - 1)
        if key in (curses.KEY_RIGHT, ord("l")):
            max_len = max((len(line) for line in rendered), default=0)
            h_offset = min(max(0, max_len - (width - 1)), h_offset + 1)
        if key in (curses.KEY_LEFT, ord("h")):
            h_offset = max(0, h_offset - 1)


def _engineer_gantt_report(stdscr, conn, unallocated_only: bool, include_provisional: bool) -> None:
    start = date.today()
    end = start.fromordinal(start.toordinal() + 90)
    engineers = _fetch_engineers(conn)
    allocations = _fetch_allocations_range(conn, start.isoformat(), end.isoformat(), include_provisional)

    # build map
    alloc_map = {e.id: [] for e in engineers}
    for alloc in allocations:
        alloc_map.setdefault(alloc["engineer_id"], []).append(alloc)

    rows = []
    for eng in engineers:
        if unallocated_only and alloc_map.get(eng.id):
            continue
        rows.append(
            {
                "engineer": eng.name,
                "engineer_id": eng.id,
                "allocations": alloc_map.get(eng.id, []),
                "obj": eng,
            }
        )

    height, width = stdscr.getmaxyx()
    gantt_rows = _gantt_rows_by_engineer(rows, start, end, width)
    offset = 0
    selected = 0
    title = "Unallocated Engineer Gantt" if unallocated_only else "Engineer Gantt"
    while True:
        start_row = _draw_header(stdscr, title, f"{start.isoformat()} -> {end.isoformat()}")
        _draw_hints(stdscr, start_row, [("j", "down"), ("k", "up"), ("o", "open"), ("b", "back")])
        start_row += 1
        visible = max(1, height - start_row - 1)
        view = gantt_rows[offset : offset + visible]
        for idx, line in enumerate(view):
            absolute = offset + idx
            attr = curses.A_REVERSE if absolute == selected else 0
            stdscr.addstr(start_row + idx, 0, line[: width - 1], attr)
        stdscr.refresh()
        key = stdscr.getch()
        if key in (27, ord("b")):
            return
        if key in (curses.KEY_DOWN, ord("j")):
            selected = min(len(rows) - 1, selected + 1)
            if selected >= offset + visible:
                offset = min(max(0, len(rows) - visible), offset + 1)
        if key in (curses.KEY_UP, ord("k")):
            selected = max(0, selected - 1)
            if selected < offset:
                offset = max(0, offset - 1)
        if key in (curses.KEY_ENTER, 10, 13, ord("o")):
            if rows:
                _engineer_screen(stdscr, conn, rows[selected]["obj"])


def _unallocated_report(stdscr, conn, include_provisional: bool) -> None:
    _engineer_gantt_report(stdscr, conn, True, include_provisional)


def _projects_ending_report(stdscr, conn, include_provisional: bool) -> None:
    rows = projects_ending_with_details(conn, date.today(), 30, include_provisional)
    _report_table(stdscr, "Projects Ending Soon", rows)


def _fetch_allocations_range(
    conn, start_iso: str, end_iso: str, include_provisional: bool = True
) -> list[dict]:
    cur = conn.execute(
        """
        SELECT a.id, a.engineer_id, a.project_id, a.start_date, a.end_date, a.status, e.name AS engineer,
               p.status AS project_status
        FROM allocation a
        JOIN engineer e ON e.id = a.engineer_id
        JOIN project p ON p.id = a.project_id
        WHERE a.start_date <= ? AND (a.end_date IS NULL OR a.end_date >= ?)
          AND (? = 1 OR (a.status = 'confirmed' AND p.status = 'confirmed'))
        ORDER BY e.name ASC, a.start_date ASC
        """,
        (end_iso, start_iso, 1 if include_provisional else 0),
    )
    return [dict(row) for row in cur.fetchall()]


def _gantt_rows_by_engineer(rows: list[dict], start: date, end: date, width: int) -> list[str]:
    total_days = (end - start).days + 1
    if total_days <= 0:
        return []
    scale = max(1, total_days // max(1, width - 24))
    bar_len = (total_days // scale) + 1
    output = []
    for row in rows:
        bar = [" "] * bar_len
        for alloc in row["allocations"]:
            a_start = date.fromisoformat(alloc["start_date"])
            a_end = date.fromisoformat(alloc["end_date"]) if alloc["end_date"] else end
            start_offset = max(0, (a_start - start).days) // scale
            end_offset = max(0, (a_end - start).days) // scale
            mark = "." if alloc.get("status") == "provisional" else "#"
            for i in range(start_offset, min(end_offset + 1, len(bar))):
                bar[i] = mark
        label = f"{row['engineer']:<20}"[:20]
        output.append(label + " " + "".join(bar))
    return output


def _scenario_list_screen(stdscr, conn) -> None:
    selected = 0
    while True:
        scenarios = list_scenarios(conn)
        items = [f"{s.name} (id {s.id})" for s in scenarios] or ["(no scenarios yet)"]
        start_row = _draw_header(stdscr, "Scenarios", None)
        _draw_hints(stdscr, start_row, [("o", "open"), ("a", "add"), ("b", "back")])
        start_row += 1
        _draw_list(stdscr, items, selected, start_row)
        stdscr.refresh()
        key = stdscr.getch()
        if key in (27, ord("b")):
            return
        if key in (curses.KEY_UP, ord("k")):
            selected = max(0, selected - 1)
        if key in (curses.KEY_DOWN, ord("j")):
            selected = min(len(items) - 1, selected + 1)
        if key == ord("a"):
            name = _prompt(stdscr, "Scenario name: ")
            if name:
                try:
                    create_scenario(conn, name)
                except Exception:
                    _draw_header(stdscr, "Error", "Scenario name must be unique.")
                    stdscr.refresh()
                    stdscr.getch()
        if key in (curses.KEY_ENTER, 10, 13, ord("o")) and scenarios:
            _scenario_screen(stdscr, conn, scenarios[selected])


def _scenario_screen(stdscr, conn, scenario) -> None:
    selected = 0
    options = [
        "What-if Revenue Year",
        "Add Tentative Project",
        "Add Allocation",
        "Update Allocation End",
        "Delete Allocation",
        "Change Engineer Rate",
        "Adjust Cell",
        "Back",
    ]
    while True:
        start_row = _draw_header(stdscr, f"Scenario: {scenario.name}", None)
        _draw_hints(
            stdscr,
            start_row,
            [
                ("w", "what-if"),
                ("p", "project"),
                ("a", "alloc"),
                ("u", "update"),
                ("d", "delete"),
                ("r", "rate"),
                ("m", "manual"),
                ("b", "back"),
            ],
        )
        start_row += 1
        _draw_list(stdscr, options, selected, start_row)
        stdscr.refresh()
        key = stdscr.getch()
        if key in (27, ord("b")):
            return
        if key in (curses.KEY_UP, ord("k")):
            selected = max(0, selected - 1)
        if key in (curses.KEY_DOWN, ord("j")):
            selected = min(len(options) - 1, selected + 1)
        if key in (curses.KEY_ENTER, 10, 13):
            choice = options[selected]
        elif key in (ord("w"), ord("p"), ord("a"), ord("u"), ord("d"), ord("r"), ord("m")):
            key_map = {
                ord("w"): "What-if Revenue Year",
                ord("p"): "Add Tentative Project",
                ord("a"): "Add Allocation",
                ord("u"): "Update Allocation End",
                ord("d"): "Delete Allocation",
                ord("r"): "Change Engineer Rate",
                ord("m"): "Adjust Cell",
            }
            choice = key_map.get(key)
        else:
            continue

        if choice == "What-if Revenue Year":
            year = _prompt(stdscr, "Year (YYYY): ")
            year_val = int(year) if year else date.today().year
            _scenario_report_table(stdscr, conn, scenario.id, year_val)
        elif choice == "Add Tentative Project":
            err = _scenario_add_project(conn, stdscr, scenario.id)
            if err:
                _draw_header(stdscr, "Error", err)
                stdscr.refresh()
                stdscr.getch()
        elif choice == "Add Allocation":
            err = _scenario_add_allocation(conn, stdscr, scenario.id)
            if err:
                _draw_header(stdscr, "Error", err)
                stdscr.refresh()
                stdscr.getch()
        elif choice == "Update Allocation End":
            err = _scenario_update_allocation(conn, stdscr, scenario.id)
            if err:
                _draw_header(stdscr, "Error", err)
                stdscr.refresh()
                stdscr.getch()
        elif choice == "Delete Allocation":
            err = _scenario_delete_allocation(conn, stdscr, scenario.id)
            if err:
                _draw_header(stdscr, "Error", err)
                stdscr.refresh()
                stdscr.getch()
        elif choice == "Change Engineer Rate":
            err = _scenario_change_rate(conn, stdscr, scenario.id)
            if err:
                _draw_header(stdscr, "Error", err)
                stdscr.refresh()
                stdscr.getch()
        elif choice == "Adjust Cell":
            err = _scenario_adjust_cell(conn, stdscr, scenario.id)
            if err:
                _draw_header(stdscr, "Error", err)
                stdscr.refresh()
                stdscr.getch()
        else:
            return


def _scenario_add_project(conn, stdscr, scenario_id: int) -> str | None:
    clients = _fetch_clients(conn)
    items = [(c.name, c) for c in clients]
    client = _select_with_search(
        stdscr,
        "Select client",
        "Type to search, Enter to select, b to cancel",
        items,
        "Client search: ",
    )
    if client is None:
        return "Cancelled."
    name = _prompt(stdscr, "Project name: ")
    start = _prompt(stdscr, "Start date (YYYY-MM-DD): ")
    end = _prompt(stdscr, "End date (YYYY-MM-DD, blank=open): ")
    rate_raw = _prompt(stdscr, "Agreed rate (blank for none): ")
    start_iso = parse_date(start)
    end_iso = parse_date(end) if end else None
    agreed_rate = float(rate_raw) if rate_raw else None
    project_id = _next_tentative_project_id(conn, scenario_id)
    add_change(
        conn,
        scenario_id,
        "project_add",
        {
            "project_id": project_id,
            "client_id": client.id,
            "name": name,
            "start_date": start_iso,
            "end_date": end_iso,
            "agreed_rate": agreed_rate,
        },
    )
    return None


def _scenario_add_allocation(conn, stdscr, scenario_id: int) -> str | None:
    projects = _scenario_project_choices(conn, scenario_id)
    p_items = [(f"{p['name']} (id {p['id']})", p) for p in projects]
    project = _select_with_search(
        stdscr,
        "Select project",
        "Type to search, Enter to select, b to cancel",
        p_items,
        "Project search: ",
    )
    if project is None:
        return "Cancelled."
    engineers = _fetch_engineers(conn)
    e_items = [(f"{e.name} (id {e.id})", e) for e in engineers]
    engineer = _select_with_search(
        stdscr,
        "Select engineer",
        "Type to search, Enter to select, b to cancel",
        e_items,
        "Engineer search: ",
    )
    if engineer is None:
        return "Cancelled."
    start = _prompt(stdscr, "Start date (YYYY-MM-DD): ")
    end = _prompt(stdscr, "End date (YYYY-MM-DD, blank=open): ")
    start_iso = parse_date(start)
    end_iso = parse_date(end) if end else None
    add_change(
        conn,
        scenario_id,
        "allocation_add",
        {
            "engineer_id": engineer.id,
            "project_id": project["id"],
            "start_date": start_iso,
            "end_date": end_iso,
        },
    )
    return None


def _scenario_update_allocation(conn, stdscr, scenario_id: int) -> str | None:
    allocs = _fetch_allocation_summaries(conn)
    if not allocs:
        return "No allocations."
    items = [(a["label"], a) for a in allocs]
    alloc = _select_with_search(
        stdscr,
        "Select allocation",
        "Type to search, Enter to select, b to cancel",
        items,
        "Allocation search: ",
    )
    if alloc is None:
        return "Cancelled."
    end = _prompt(stdscr, "New end date (YYYY-MM-DD, blank=open): ")
    end_iso = parse_date(end) if end else None
    add_change(
        conn,
        scenario_id,
        "allocation_update",
        {"allocation_id": alloc["id"], "end_date": end_iso},
    )
    return None


def _scenario_delete_allocation(conn, stdscr, scenario_id: int) -> str | None:
    allocs = _fetch_allocation_summaries(conn)
    if not allocs:
        return "No allocations."
    items = [(a["label"], a) for a in allocs]
    alloc = _select_with_search(
        stdscr,
        "Select allocation",
        "Type to search, Enter to select, b to cancel",
        items,
        "Allocation search: ",
    )
    if alloc is None:
        return "Cancelled."
    add_change(
        conn,
        scenario_id,
        "allocation_delete",
        {"allocation_id": alloc["id"]},
    )
    return None


def _scenario_change_rate(conn, stdscr, scenario_id: int) -> str | None:
    engineers = _fetch_engineers(conn)
    items = [(f"{e.name} (id {e.id})", e) for e in engineers]
    engineer = _select_with_search(
        stdscr,
        "Select engineer",
        "Type to search, Enter to select, b to cancel",
        items,
        "Engineer search: ",
    )
    if engineer is None:
        return "Cancelled."
    rate_raw = _prompt(stdscr, "New day rate: ")
    if not rate_raw:
        return "Rate required."
    new_rate = float(rate_raw)
    add_change(
        conn,
        scenario_id,
        "engineer_rate",
        {"engineer_id": engineer.id, "new_day_rate": new_rate},
    )
    return None


def _scenario_adjust_cell(conn, stdscr, scenario_id: int) -> str | None:
    clients = _fetch_clients(conn)
    items = [(c.name, c) for c in clients]
    client = _select_with_search(
        stdscr,
        "Select client",
        "Type to search, Enter to select, b to cancel",
        items,
        "Client search: ",
    )
    if client is None:
        return "Cancelled."
    month = _prompt(stdscr, "Month (Jan..Dec or total): ")
    if not month:
        return "Month required."
    month = month.strip().title()
    if month.lower() == "total":
        month = "total"
    amount_raw = _prompt(stdscr, "Adjustment amount (e.g. 1200 or -300): ")
    if not amount_raw:
        return "Amount required."
    amount = float(amount_raw)
    add_change(
        conn,
        scenario_id,
        "cell_adjust",
        {"client_id": client.id, "month": month, "amount": amount},
    )
    return None


def _fetch_allocation_summaries(conn) -> list[dict]:
    cur = conn.execute(
        """
        SELECT a.id, e.name AS engineer, p.name AS project, a.start_date, a.end_date
        FROM allocation a
        JOIN engineer e ON e.id = a.engineer_id
        JOIN project p ON p.id = a.project_id
        ORDER BY a.start_date DESC
        """
    )
    results = []
    for row in cur.fetchall():
        end_label = row["end_date"] if row["end_date"] else "open"
        label = f"{row['engineer']} on {row['project']} {row['start_date']}->{end_label} (id {row['id']})"
        results.append({"id": row["id"], "label": label})
    return results


def _scenario_report_table(stdscr, conn, scenario_id: int, year: int) -> None:
    data = scenario_client_revenue_year(conn, scenario_id, year)
    rows = data["rows"]
    dirty = data["dirty"]
    cell_changes = data["cell_changes"]
    if not rows:
        _draw_header(stdscr, "Scenario Report", "(no data)")
        stdscr.refresh()
        stdscr.getch()
        return

    headers = [h for h in rows[0].keys() if h != "client_id"]
    selected_row = 0
    selected_col = 0
    v_offset = 0
    h_offset = 0

    def fmt(val):
        if isinstance(val, float):
            return f"{val:.2f}"
        return str(val)

    while True:
        start_row = _draw_header(stdscr, f"Scenario Revenue {year}", None)
        _draw_hints(
            stdscr,
            start_row,
            [("↑", "up"), ("↓", "down"), ("←", "left"), ("→", "right"), ("o", "open"), ("b", "back")],
        )
        start_row += 1
        sel_col_name = headers[min(selected_col, len(headers) - 1)]
        sel_row = rows[min(selected_row, len(rows) - 1)]
        stdscr.addstr(start_row, 0, f"Selected: {sel_row.get('client','')} / {sel_col_name}")
        start_row += 1
        height, width = stdscr.getmaxyx()

        # Build plain table with box-drawing chars so we can highlight selected cell
        col_widths = {h: len(h) for h in headers}
        for row in rows:
            for h in headers:
                col_widths[h] = max(col_widths[h], len(fmt(row[h])))

        def hline(left: str, mid: str, right: str) -> str:
            parts = []
            for idx, h in enumerate(headers):
                parts.append("─" * (col_widths[h] + 2))
                if idx != len(headers) - 1:
                    parts.append(mid)
            return left + "".join(parts) + right

        top = hline("┌", "┬", "┐")
        sep = hline("├", "┼", "┤")
        bot = hline("└", "┴", "┘")
        header_cells = [f" {h.ljust(col_widths[h])} " for h in headers]
        header_line = "│" + "│".join(header_cells) + "│"

        rendered = [top, header_line, sep]
        for row in rows:
            cells = []
            for h in headers:
                val = fmt(row[h])
                cells.append(f" {val.rjust(col_widths[h]) if h != 'client' else val.ljust(col_widths[h])} ")
            rendered.append("│" + "│".join(cells) + "│")
        rendered.append(bot)

        visible = max(1, height - start_row - 1)
        view = rendered[v_offset : v_offset + visible]
        for idx, line in enumerate(view):
            clipped = line[h_offset : h_offset + width - 1]
            stdscr.addstr(start_row + idx, 0, clipped)

        # Highlight dirty cells for visible rows based on line indices
        for row_index, row in enumerate(rows):
            table_line_index = 3 + row_index  # top, header, sep
            if table_line_index < v_offset or table_line_index >= v_offset + visible:
                continue
            client_id = row.get("client_id")
            is_total = row.get("client") == "TOTAL"
            cell_start = 1
            for i, h in enumerate(headers):
                cell_len = col_widths[h] + 2
                cell_end = cell_start + cell_len
                vis_start = max(cell_start, h_offset)
                vis_end = min(cell_end, h_offset + width - 1)
                if vis_start < vis_end:
                    text = rendered[table_line_index][vis_start:vis_end]
                    attr = 0
                    dirty_client = client_id if client_id is not None else None
                    if (dirty_client in dirty and h in dirty[dirty_client]):
                        attr |= curses.A_BOLD
                    if row_index == selected_row and i == selected_col:
                        attr |= curses.A_REVERSE
                    if attr:
                        screen_y = start_row + (table_line_index - v_offset)
                        screen_x = vis_start - h_offset
                        stdscr.addstr(screen_y, screen_x, text, attr)
                cell_start = cell_end + 1

        stdscr.refresh()
        key = stdscr.getch()
        if key in (27, ord("b")):
            return
        if key in (curses.KEY_DOWN,):
            selected_row = min(len(rows) - 1, selected_row + 1)
            table_line_index = 3 + selected_row
            if table_line_index >= v_offset + visible:
                v_offset = min(max(0, len(rendered) - visible), v_offset + 1)
        if key in (curses.KEY_UP,):
            selected_row = max(0, selected_row - 1)
            table_line_index = 3 + selected_row
            if table_line_index < v_offset:
                v_offset = max(0, v_offset - 1)
        if key in (curses.KEY_RIGHT,):
            selected_col = min(len(headers) - 1, selected_col + 1)
        if key in (curses.KEY_LEFT,):
            selected_col = max(0, selected_col - 1)
        if key in (curses.KEY_ENTER, 10, 13, ord("o")):
            row = rows[selected_row]
            client_id = row.get("client_id")
            if client_id is None:
                _scenario_cell_detail(stdscr, client_id, sel_col_name, cell_changes)
                continue
            col = headers[min(selected_col, len(headers) - 1)]
            _scenario_cell_detail(stdscr, client_id, col, cell_changes)
        # auto horizontal scroll to keep selected cell visible
        cell_start = 1
        for i, h in enumerate(headers):
            cell_len = col_widths[h] + 2
            cell_end = cell_start + cell_len
            if i == selected_col:
                if cell_start < h_offset:
                    h_offset = cell_start
                elif cell_end > h_offset + (width - 1):
                    h_offset = max(0, cell_end - (width - 1))
                break
            cell_start = cell_end + 1


def _scenario_cell_detail(stdscr, client_id: int | None, column: str, cell_changes: dict) -> None:
    key = (client_id, column)
    changes = cell_changes.get(key, [])
    if not changes:
        changes = ["No scenario changes affect this cell."]
    while True:
        start_row = _draw_header(stdscr, f"Cell Detail: {column}", None)
        _draw_hints(stdscr, start_row, [("b", "back")])
        start_row += 1
        height, width = stdscr.getmaxyx()
        for idx, line in enumerate(changes[: max(1, height - start_row - 1)]):
            stdscr.addstr(start_row + idx, 0, line[: width - 1])
        stdscr.refresh()
        key = stdscr.getch()
        if key in (27, ord("b")):
            return


def _scenario_project_choices(conn, scenario_id: int) -> list[dict]:
    base_projects = _fetch_projects(conn)
    projects = [{"id": p.id, "name": p.name} for p in base_projects]
    changes = list_changes(conn, scenario_id)
    for change in changes:
        if change["change_type"] == "project_add":
            payload = change["payload"]
            projects.append({"id": payload["project_id"], "name": payload["name"]})
        if change["change_type"] == "project_delete":
            proj_id = change["payload"].get("project_id")
            projects = [p for p in projects if p["id"] != proj_id]
    return projects


def _next_tentative_project_id(conn, scenario_id: int) -> int:
    changes = list_changes(conn, scenario_id)
    min_id = 0
    for change in changes:
        if change["change_type"] == "project_add":
            pid = change["payload"].get("project_id", 0)
            if isinstance(pid, int):
                min_id = min(min_id, pid)
    return min_id - 1 if min_id <= 0 else -1


def _main_menu(stdscr, conn):
    options = ["Projects", "Engineers", "Clients", "Reports", "Config", "Quit"]
    selected = 0
    while True:
        start_row = _draw_header(stdscr, "Turntabl TUI", None)
        _draw_hints(
            stdscr,
            start_row,
            [
                ("p", "projects"),
                ("e", "engineers"),
                ("c", "clients"),
                ("r", "reports"),
                ("o", "config"),
                ("q", "quit"),
            ],
        )
        start_row += 1
        _draw_list(stdscr, options, selected, start_row)
        stdscr.refresh()
        key = stdscr.getch()
        if key in (curses.KEY_UP, ord("k")):
            selected = max(0, selected - 1)
        elif key in (curses.KEY_DOWN, ord("j")):
            selected = min(len(options) - 1, selected + 1)
        elif key in (curses.KEY_ENTER, 10, 13):
            if options[selected] == "Projects":
                _projects_list(stdscr, conn)
            elif options[selected] == "Engineers":
                _engineers_list(stdscr, conn)
            elif options[selected] == "Clients":
                _clients_list(stdscr, conn)
            elif options[selected] == "Reports":
                _reports_screen(stdscr, conn)
            elif options[selected] == "Config":
                _config_screen(stdscr)
            else:
                return
        elif key in (27, ord("q")):
            return
        elif key == ord("p"):
            _projects_list(stdscr, conn)
        elif key == ord("e"):
            _engineers_list(stdscr, conn)
        elif key == ord("c"):
            _clients_list(stdscr, conn)
        elif key == ord("r"):
            _reports_screen(stdscr, conn)
        elif key == ord("o"):
            _config_screen(stdscr)


def run() -> None:
    conn = connect()
    init_db(conn)

    def _wrapped(stdscr):
        global ADVANCED_MODE, COLOR_ENABLED
        if curses.has_colors():
            curses.start_color()
            curses.use_default_colors()
            curses.init_pair(1, curses.COLOR_CYAN, -1)
            curses.init_pair(2, curses.COLOR_YELLOW, -1)
            COLOR_ENABLED = True
        try:
            curses.curs_set(0)
        except curses.error:
            pass
        stdscr.keypad(True)
        _load_config()
        _main_menu(stdscr, conn)

    try:
        curses.wrapper(_wrapped)
    finally:
        conn.close()


if __name__ == "__main__":
    run()
