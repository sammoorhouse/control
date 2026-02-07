from __future__ import annotations

from datetime import date
from typing import Optional

import typer

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
from .reports import (
    current_allocations,
    projects_ending_soon,
    projects_with_no_allocations,
    report_client_revenue,
    report_engineer_revenue,
    report_project_revenue,
    report_client_revenue_year,
    projects_ending_with_details,
    unallocated_engineers,
)

app = typer.Typer(help="Turntabl staffing and allocation CLI")

engineer_app = typer.Typer(help="Manage engineers")
client_app = typer.Typer(help="Manage clients")
contact_app = typer.Typer(help="Manage contacts")
project_app = typer.Typer(help="Manage projects")
allocation_app = typer.Typer(help="Manage allocations")
report_app = typer.Typer(help="Generate reports")

app.add_typer(engineer_app, name="engineer")
app.add_typer(client_app, name="client")
app.add_typer(contact_app, name="contact")
app.add_typer(project_app, name="project")
app.add_typer(allocation_app, name="allocation")
app.add_typer(report_app, name="report")


def _ensure_db():
    conn = connect()
    init_db(conn)
    return conn


def _print_rows(rows: list[dict]) -> None:
    if not rows:
        typer.echo("(no results)")
        return
    headers = list(rows[0].keys())
    typer.echo("\t".join(headers))
    for row in rows:
        typer.echo("\t".join(str(row[h]) if row[h] is not None else "" for h in headers))


@app.command("init-db")
def init_db_command():
    """Initialize the local database schema."""
    conn = _ensure_db()
    conn.close()
    typer.echo("Database initialized.")


@app.command("tui")
def tui_command():
    """Launch the interactive terminal UI."""
    from .tui import run

    run()


@engineer_app.command("add")
def add_engineer(
    name: str,
    level: int = typer.Option(..., min=1, max=5),
    cohort: int = typer.Option(..., min=1, max=8),
    day_rate: Optional[float] = None,
    active: bool = True,
):
    """Add an engineer."""
    conn = _ensure_db()
    cohort_id = ensure_cohort(conn, cohort)
    cur = conn.execute(
        """
        INSERT INTO engineer (name, level, day_rate, cohort_id, active)
        VALUES (?, ?, ?, ?, ?)
        """,
        (name, level, day_rate, cohort_id, 1 if active else 0),
    )
    conn.commit()
    conn.close()
    typer.echo(f"Engineer added with id {cur.lastrowid}.")


@engineer_app.command("remove")
def remove_engineer(engineer_id: int):
    """Remove an engineer."""
    conn = _ensure_db()
    cur = conn.execute("DELETE FROM engineer WHERE id = ?", (engineer_id,))
    conn.commit()
    conn.close()
    if cur.rowcount == 0:
        raise typer.Exit(code=1)
    typer.echo("Engineer removed.")


@client_app.command("add")
def add_client(name: str):
    """Add a client."""
    conn = _ensure_db()
    cur = conn.execute("INSERT INTO client (name) VALUES (?)", (name,))
    conn.commit()
    conn.close()
    typer.echo(f"Client added with id {cur.lastrowid}.")


@client_app.command("remove")
def remove_client(client_id: int):
    """Remove a client."""
    conn = _ensure_db()
    cur = conn.execute("DELETE FROM client WHERE id = ?", (client_id,))
    conn.commit()
    conn.close()
    if cur.rowcount == 0:
        raise typer.Exit(code=1)
    typer.echo("Client removed.")


@contact_app.command("add")
def add_contact(
    client_id: int,
    name: str,
    email: Optional[str] = None,
    phone: Optional[str] = None,
):
    """Add a contact for a client."""
    conn = _ensure_db()
    cur = conn.execute(
        """
        INSERT INTO contact (client_id, name, email, phone)
        VALUES (?, ?, ?, ?)
        """,
        (client_id, name, email, phone),
    )
    conn.commit()
    conn.close()
    typer.echo(f"Contact added with id {cur.lastrowid}.")


@contact_app.command("remove")
def remove_contact(contact_id: int):
    """Remove a contact."""
    conn = _ensure_db()
    cur = conn.execute("DELETE FROM contact WHERE id = ?", (contact_id,))
    conn.commit()
    conn.close()
    if cur.rowcount == 0:
        raise typer.Exit(code=1)
    typer.echo("Contact removed.")


@project_app.command("add")
def add_project(
    client_id: int,
    name: str,
    start_date: str,
    end_date: str,
    agreed_rate: Optional[float] = None,
    status: str = typer.Option("confirmed", case_insensitive=True),
):
    """Add a project."""
    conn = _ensure_db()
    start_iso = parse_date(start_date)
    end_iso = None
    if end_date.lower() not in ("open", "none", "-"):
        end_iso = parse_date(end_date)
        if start_iso > end_iso:
            raise DbError("Project start_date must be on or before end_date.")
    if status not in ("confirmed", "provisional"):
        raise DbError("Status must be 'confirmed' or 'provisional'.")
    cur = conn.execute(
        """
        INSERT INTO project (client_id, name, start_date, end_date, agreed_rate, status)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (client_id, name, start_iso, end_iso, agreed_rate, status),
    )
    conn.commit()
    conn.close()
    typer.echo(f"Project added with id {cur.lastrowid}.")


@project_app.command("remove")
def remove_project(project_id: int):
    """Remove a project."""
    conn = _ensure_db()
    cur = conn.execute("DELETE FROM project WHERE id = ?", (project_id,))
    conn.commit()
    conn.close()
    if cur.rowcount == 0:
        raise typer.Exit(code=1)
    typer.echo("Project removed.")


@allocation_app.command("add")
def add_allocation(
    engineer_id: int,
    project_id: int,
    start_date: str,
    end_date: str,
    status: Optional[str] = typer.Option(None, case_insensitive=True),
):
    """Allocate an engineer to a project for a date range."""
    conn = _ensure_db()
    start_iso = parse_date(start_date)
    end_iso = None
    if end_date.lower() not in ("open", "none", "-"):
        end_iso = parse_date(end_date)
        if start_iso > end_iso:
            raise DbError("Allocation start_date must be on or before end_date.")
    validate_engineer_exists(conn, engineer_id)
    validate_project_exists(conn, project_id)
    if status is None:
        row = conn.execute("SELECT status FROM project WHERE id = ?", (project_id,)).fetchone()
        status = row["status"] if row and row["status"] else "confirmed"
    if status not in ("confirmed", "provisional"):
        raise DbError("Status must be 'confirmed' or 'provisional'.")
    if end_iso is None:
        cur = conn.execute("SELECT end_date FROM project WHERE id = ?", (project_id,))
        row = cur.fetchone()
        if row and row["end_date"] is not None:
            raise DbError("Open-ended allocations require an open-ended project.")
        validate_project_window(conn, project_id, start_iso, start_iso)
    else:
        validate_project_window(conn, project_id, start_iso, end_iso)
    cur = conn.execute(
        """
        INSERT INTO allocation (engineer_id, project_id, start_date, end_date, status)
        VALUES (?, ?, ?, ?, ?)
        """,
        (engineer_id, project_id, start_iso, end_iso, status),
    )
    conn.commit()
    conn.close()
    typer.echo(f"Allocation added with id {cur.lastrowid}.")


@allocation_app.command("remove")
def remove_allocation(allocation_id: int):
    """Remove an allocation."""
    conn = _ensure_db()
    cur = conn.execute("DELETE FROM allocation WHERE id = ?", (allocation_id,))
    conn.commit()
    conn.close()
    if cur.rowcount == 0:
        raise typer.Exit(code=1)
    typer.echo("Allocation removed.")


@report_app.command("unallocated")
def report_unallocated(
    as_of: Optional[str] = None,
    include_provisional: bool = typer.Option(True, "--include-provisional/--exclude-provisional"),
):
    """List currently unallocated engineers."""
    conn = _ensure_db()
    day = date.fromisoformat(as_of) if as_of else date.today()
    rows = unallocated_engineers(conn, day, include_provisional)
    conn.close()
    _print_rows(rows)


@report_app.command("projects-ending")
def report_projects_ending(
    within: int = typer.Option(30, min=1),
    as_of: Optional[str] = None,
    include_provisional: bool = typer.Option(True, "--include-provisional/--exclude-provisional"),
):
    """List projects ending soon."""
    conn = _ensure_db()
    day = date.fromisoformat(as_of) if as_of else date.today()
    rows = projects_ending_soon(conn, day, within, include_provisional)
    conn.close()
    _print_rows(rows)


@report_app.command("projects-no-allocations")
def report_projects_no_allocations(
    include_provisional: bool = typer.Option(True, "--include-provisional/--exclude-provisional"),
):
    """List projects with no allocations."""
    conn = _ensure_db()
    rows = projects_with_no_allocations(conn, include_provisional)
    conn.close()
    _print_rows(rows)


@report_app.command("allocations")
def report_allocations(
    as_of: Optional[str] = None,
    include_provisional: bool = typer.Option(True, "--include-provisional/--exclude-provisional"),
):
    """List currently allocated engineers with project details."""
    conn = _ensure_db()
    day = date.fromisoformat(as_of) if as_of else date.today()
    rows = current_allocations(conn, day, include_provisional)
    conn.close()
    _print_rows(rows)


@report_app.command("project-revenue")
def report_project_revenue_command(
    as_of: Optional[str] = None,
    include_provisional: bool = typer.Option(True, "--include-provisional/--exclude-provisional"),
):
    """Project revenue to date and total."""
    conn = _ensure_db()
    day = date.fromisoformat(as_of) if as_of else date.today()
    rows = report_project_revenue(conn, day, include_provisional)
    conn.close()
    _print_rows(rows)


@report_app.command("client-revenue")
def report_client_revenue_command(
    as_of: Optional[str] = None,
    include_provisional: bool = typer.Option(True, "--include-provisional/--exclude-provisional"),
):
    """Client revenue to date and total."""
    conn = _ensure_db()
    day = date.fromisoformat(as_of) if as_of else date.today()
    rows = report_client_revenue(conn, day, include_provisional)
    conn.close()
    _print_rows(rows)


@report_app.command("engineer-revenue")
def report_engineer_revenue_command(
    as_of: Optional[str] = None,
    include_provisional: bool = typer.Option(True, "--include-provisional/--exclude-provisional"),
):
    """Engineer revenue to date and total."""
    conn = _ensure_db()
    day = date.fromisoformat(as_of) if as_of else date.today()
    rows = report_engineer_revenue(conn, day, include_provisional)
    conn.close()
    _print_rows(rows)


@report_app.command("client-revenue-year")
def report_client_revenue_year_command(
    year: int = typer.Option(date.today().year, min=2000, max=2100),
    include_provisional: bool = typer.Option(True, "--include-provisional/--exclude-provisional"),
):
    """Client revenue by month for a year."""
    conn = _ensure_db()
    rows = report_client_revenue_year(conn, year, include_provisional)
    conn.close()
    _print_rows(rows)


@report_app.command("projects-ending-details")
def report_projects_ending_details(
    within: int = typer.Option(30, min=1),
    as_of: Optional[str] = None,
    include_provisional: bool = typer.Option(True, "--include-provisional/--exclude-provisional"),
):
    """Projects ending soon with allocations and revenue."""
    conn = _ensure_db()
    day = date.fromisoformat(as_of) if as_of else date.today()
    rows = projects_ending_with_details(conn, day, within, include_provisional)
    conn.close()
    _print_rows(rows)


@app.callback()
def main() -> None:
    pass


if __name__ == "__main__":
    app()
