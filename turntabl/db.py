from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable

DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "turntabl.db"


@dataclass(frozen=True)
class DbConfig:
    path: Path


class DbError(Exception):
    pass


def get_db_path() -> Path:
    env = os.getenv("TURNTABL_DB")
    if env:
        return Path(env).expanduser().resolve()
    return DEFAULT_DB_PATH


def connect(db_path: Path | None = None) -> sqlite3.Connection:
    path = db_path or get_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS tlc_cohort (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            number INTEGER NOT NULL UNIQUE CHECK (number BETWEEN 1 AND 8)
        );

        CREATE TABLE IF NOT EXISTS engineer (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            level INTEGER NOT NULL CHECK (level BETWEEN 1 AND 5),
            day_rate REAL,
            cohort_id INTEGER NOT NULL REFERENCES tlc_cohort(id) ON DELETE RESTRICT,
            active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0,1))
        );

        CREATE TABLE IF NOT EXISTS client (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS contact (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER NOT NULL REFERENCES client(id) ON DELETE CASCADE,
            name TEXT NOT NULL,
            email TEXT,
            phone TEXT
        );

        CREATE TABLE IF NOT EXISTS project (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER NOT NULL REFERENCES client(id) ON DELETE CASCADE,
            name TEXT NOT NULL,
            start_date TEXT NOT NULL,
            end_date TEXT,
            agreed_rate REAL,
            status TEXT NOT NULL DEFAULT 'confirmed' CHECK (status IN ('confirmed','provisional')),
            CHECK (end_date IS NULL OR start_date <= end_date)
        );

        CREATE TABLE IF NOT EXISTS allocation (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            engineer_id INTEGER NOT NULL REFERENCES engineer(id) ON DELETE CASCADE,
            project_id INTEGER NOT NULL REFERENCES project(id) ON DELETE CASCADE,
            start_date TEXT NOT NULL,
            end_date TEXT,
            status TEXT NOT NULL DEFAULT 'confirmed' CHECK (status IN ('confirmed','provisional')),
            CHECK (end_date IS NULL OR start_date <= end_date)
        );

        CREATE TABLE IF NOT EXISTS scenario (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS scenario_change (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scenario_id INTEGER NOT NULL REFERENCES scenario(id) ON DELETE CASCADE,
            change_type TEXT NOT NULL,
            payload TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_allocation_engineer_dates
            ON allocation (engineer_id, start_date, end_date);

        CREATE INDEX IF NOT EXISTS idx_allocation_project_dates
            ON allocation (project_id, start_date, end_date);
        """
    )
    _migrate_nullable_end_dates(conn)
    _migrate_status_columns(conn)
    conn.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_allocation_engineer_dates
            ON allocation (engineer_id, start_date, end_date);

        CREATE INDEX IF NOT EXISTS idx_allocation_project_dates
            ON allocation (project_id, start_date, end_date);

        CREATE INDEX IF NOT EXISTS idx_scenario_change_scenario
            ON scenario_change (scenario_id, created_at);
        """
    )
    conn.commit()


def _column_notnull(conn: sqlite3.Connection, table: str, column: str) -> int | None:
    cur = conn.execute(f"PRAGMA table_info({table})")
    for row in cur.fetchall():
        if row["name"] == column:
            return int(row["notnull"])
    return None


def _migrate_nullable_end_dates(conn: sqlite3.Connection) -> None:
    # If existing schema has NOT NULL end_date, recreate tables with nullable end_date.
    project_notnull = _column_notnull(conn, "project", "end_date")
    allocation_notnull = _column_notnull(conn, "allocation", "end_date")

    if project_notnull == 1:
        conn.executescript(
            """
            ALTER TABLE project RENAME TO project_old;
            CREATE TABLE project (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id INTEGER NOT NULL REFERENCES client(id) ON DELETE CASCADE,
                name TEXT NOT NULL,
                start_date TEXT NOT NULL,
                end_date TEXT,
                agreed_rate REAL,
                status TEXT NOT NULL DEFAULT 'confirmed' CHECK (status IN ('confirmed','provisional')),
                CHECK (end_date IS NULL OR start_date <= end_date)
            );
            INSERT INTO project (id, client_id, name, start_date, end_date, agreed_rate, status)
            SELECT id, client_id, name, start_date, end_date, agreed_rate, 'confirmed'
            FROM project_old;
            DROP TABLE project_old;
            """
        )

    if allocation_notnull == 1:
        conn.executescript(
            """
            ALTER TABLE allocation RENAME TO allocation_old;
            CREATE TABLE allocation (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                engineer_id INTEGER NOT NULL REFERENCES engineer(id) ON DELETE CASCADE,
                project_id INTEGER NOT NULL REFERENCES project(id) ON DELETE CASCADE,
                start_date TEXT NOT NULL,
                end_date TEXT,
                status TEXT NOT NULL DEFAULT 'confirmed' CHECK (status IN ('confirmed','provisional')),
                CHECK (end_date IS NULL OR start_date <= end_date)
            );
            INSERT INTO allocation (id, engineer_id, project_id, start_date, end_date, status)
            SELECT id, engineer_id, project_id, start_date, end_date, 'confirmed'
            FROM allocation_old;
            DROP TABLE allocation_old;
            """
        )


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return any(row["name"] == column for row in cur.fetchall())


def _migrate_status_columns(conn: sqlite3.Connection) -> None:
    if not _column_exists(conn, "project", "status"):
        conn.execute(
            "ALTER TABLE project ADD COLUMN status TEXT NOT NULL DEFAULT 'confirmed' CHECK (status IN ('confirmed','provisional'))"
        )
    if not _column_exists(conn, "allocation", "status"):
        conn.execute(
            "ALTER TABLE allocation ADD COLUMN status TEXT NOT NULL DEFAULT 'confirmed' CHECK (status IN ('confirmed','provisional'))"
        )


def parse_date(value: str) -> str:
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise DbError(f"Invalid date '{value}'. Use YYYY-MM-DD.") from exc


def ensure_cohort(conn: sqlite3.Connection, number: int) -> int:
    cur = conn.execute("SELECT id FROM tlc_cohort WHERE number = ?", (number,))
    row = cur.fetchone()
    if row:
        return int(row["id"])
    cur = conn.execute("INSERT INTO tlc_cohort (number) VALUES (?)", (number,))
    conn.commit()
    return int(cur.lastrowid)


def validate_project_window(conn: sqlite3.Connection, project_id: int, start_date: str, end_date: str) -> None:
    cur = conn.execute(
        "SELECT start_date, end_date FROM project WHERE id = ?",
        (project_id,),
    )
    row = cur.fetchone()
    if not row:
        raise DbError(f"Project {project_id} does not exist.")
    project_end = row["end_date"]
    if start_date < row["start_date"]:
        raise DbError(
            "Allocation dates must fall within the project window "
            f"({row['start_date']} to {project_end or 'open-ended'})."
        )
    if project_end is not None and end_date > project_end:
        raise DbError(
            "Allocation dates must fall within the project window "
            f"({row['start_date']} to {project_end})."
        )


def validate_engineer_exists(conn: sqlite3.Connection, engineer_id: int) -> None:
    cur = conn.execute("SELECT id FROM engineer WHERE id = ?", (engineer_id,))
    if not cur.fetchone():
        raise DbError(f"Engineer {engineer_id} does not exist.")


def validate_project_exists(conn: sqlite3.Connection, project_id: int) -> None:
    cur = conn.execute("SELECT id FROM project WHERE id = ?", (project_id,))
    if not cur.fetchone():
        raise DbError(f"Project {project_id} does not exist.")


def rows_to_dicts(rows: Iterable[sqlite3.Row]) -> list[dict]:
    return [dict(row) for row in rows]
