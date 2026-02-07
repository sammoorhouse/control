from __future__ import annotations

from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from turntabl.cli import app
from turntabl.db import connect, init_db, parse_date, validate_project_window, DbError


runner = CliRunner()


def make_env(tmp_path: Path) -> dict:
    return {"TURNTABL_DB": str(tmp_path / "turntabl-test.db")}


def test_allocation_reports_and_unallocated(tmp_path: Path):
    env = make_env(tmp_path)

    result = runner.invoke(app, ["init-db"], env=env)
    assert result.exit_code == 0

    result = runner.invoke(app, ["client", "add", "Acme"], env=env)
    assert result.exit_code == 0

    result = runner.invoke(
        app,
        [
            "project",
            "add",
            "1",
            "Phoenix",
            "2026-02-01",
            "2026-02-28",
            "--agreed-rate",
            "1200",
        ],
        env=env,
    )
    assert result.exit_code == 0

    result = runner.invoke(
        app,
        [
            "engineer",
            "add",
            "Ava",
            "--level",
            "3",
            "--cohort",
            "4",
            "--day-rate",
            "900",
        ],
        env=env,
    )
    assert result.exit_code == 0

    result = runner.invoke(
        app,
        [
            "allocation",
            "add",
            "1",
            "1",
            "2026-02-10",
            "2026-02-20",
        ],
        env=env,
    )
    assert result.exit_code == 0

    result = runner.invoke(
        app,
        ["report", "allocations", "--as-of", "2026-02-15"],
        env=env,
    )
    assert result.exit_code == 0
    assert "Ava" in result.output
    assert "Phoenix" in result.output

    result = runner.invoke(
        app,
        ["report", "unallocated", "--as-of", "2026-02-15"],
        env=env,
    )
    assert result.exit_code == 0
    assert "Ava" not in result.output



def test_projects_ending_and_no_allocations(tmp_path: Path):
    env = make_env(tmp_path)
    runner.invoke(app, ["init-db"], env=env)

    runner.invoke(app, ["client", "add", "Beta"], env=env)

    runner.invoke(
        app,
        [
            "project",
            "add",
            "1",
            "Orbit",
            "2026-02-01",
            "2026-02-12",
        ],
        env=env,
    )

    result = runner.invoke(
        app,
        ["report", "projects-ending", "--within", "14", "--as-of", "2026-02-05"],
        env=env,
    )
    assert result.exit_code == 0
    assert "Orbit" in result.output

    result = runner.invoke(app, ["report", "projects-no-allocations"], env=env)
    assert result.exit_code == 0
    assert "Orbit" in result.output


def test_allocation_window_validation(tmp_path: Path):
    db_path = tmp_path / "db.sqlite"
    conn = connect(db_path)
    init_db(conn)

    conn.execute("INSERT INTO client (name) VALUES ('Gamma')")
    conn.execute(
        "INSERT INTO project (client_id, name, start_date, end_date) VALUES (1, 'Apollo', '2026-02-01', '2026-02-10')"
    )
    conn.commit()

    start = parse_date("2026-01-25")
    end = parse_date("2026-02-05")
    try:
        validate_project_window(conn, 1, start, end)
    except DbError as exc:
        assert "within the project window" in str(exc)
    else:
        raise AssertionError("Expected window validation to fail")

    conn.close()


def test_everything_year_report_hierarchy_and_at_risk(tmp_path: Path):
    env = make_env(tmp_path)

    assert runner.invoke(app, ["init-db"], env=env).exit_code == 0
    assert runner.invoke(app, ["client", "add", "Acme"], env=env).exit_code == 0
    assert (
        runner.invoke(
            app,
            [
                "project",
                "add",
                "1",
                "Phoenix",
                "2026-01-01",
                "2026-03-31",
                "--agreed-rate",
                "1200",
                "--status",
                "provisional",
            ],
            env=env,
        ).exit_code
        == 0
    )
    assert (
        runner.invoke(
            app,
            ["engineer", "add", "Ava", "--level", "3", "--cohort", "2", "--day-rate", "950"],
            env=env,
        ).exit_code
        == 0
    )
    assert (
        runner.invoke(
            app,
            ["allocation", "add", "1", "1", "2026-01-20", "2026-02-10", "--status", "provisional"],
            env=env,
        ).exit_code
        == 0
    )

    result = runner.invoke(app, ["report", "everything-year", "--year", "2026"], env=env)
    assert result.exit_code == 0
    lines = [line for line in result.output.splitlines() if line.strip()]

    assert lines[0].startswith("row_id\tparent_id\trow_type\tlabel\tat_risk\texpandable\tJan\tFeb")
    assert "client:1\t\tclient\tAcme\t0\t1\t14400.0\t12000.0" in result.output
    assert "project:1\tclient:1\tproject\tPhoenix\t1\t1\t14400.0\t12000.0" in result.output
    assert "allocation:1\tproject:1\tallocation\tAva 2026-01-20->2026-02-10\t1\t0\t14400.0\t12000.0" in result.output
    assert "total\t\ttotal\tTOTAL\t0\t0\t14400.0\t12000.0" in result.output

    # Excluding provisional rows removes project/allocation values.
    result = runner.invoke(
        app,
        ["report", "everything-year", "--year", "2026", "--exclude-provisional"],
        env=env,
    )
    assert result.exit_code == 0
    assert "project:1" not in result.output
    assert "allocation:1" not in result.output
    assert "client:1\t\tclient\tAcme\t0\t0\t0.0\t0.0" in result.output
