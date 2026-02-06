from __future__ import annotations

from datetime import date

from turntabl.tui import _gantt_rows, _render_engineer_gaps


def test_gantt_rows_generates_bars():
    allocs = [
        {"engineer": "Ava", "start_date": "2026-02-01", "end_date": "2026-02-05"},
        {"engineer": "Ben", "start_date": "2026-02-03", "end_date": "2026-02-10"},
    ]
    rows = _gantt_rows(allocs, date(2026, 2, 1), date(2026, 2, 10), width=60)
    assert len(rows) == 2
    assert "Ava" in rows[0]
    assert "#" in rows[0]


def test_render_engineer_gaps():
    allocs = [
        {"start_date": "2026-02-01", "end_date": "2026-02-05"},
        {"start_date": "2026-02-10", "end_date": "2026-02-12"},
    ]
    gaps = _render_engineer_gaps(allocs)
    assert any("Gap:" in g for g in gaps)
