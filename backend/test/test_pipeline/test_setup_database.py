"""
Tests for setup_database.py to guarantee full coverage.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import List

import pytest

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from app.pipeline import setup_database  # noqa: E402


def test_get_database_path_env_absolute(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    absolute_path = tmp_path / "custom.db"
    monkeypatch.setenv("database_path", str(absolute_path))

    resolved = setup_database.get_database_path()

    assert resolved == absolute_path


def test_get_database_path_env_relative(monkeypatch: pytest.MonkeyPatch) -> None:
    relative = "relative/location.db"
    monkeypatch.setenv("database_path", relative)

    resolved = setup_database.get_database_path()

    expected = setup_database.BACKEND_ROOT / relative
    assert resolved == expected
    assert resolved.is_absolute()


def test_get_database_path_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("database_path", raising=False)

    resolved = setup_database.get_database_path()

    assert resolved == setup_database.DEFAULT_DB_PATH


def test_apply_schema_executes_statements() -> None:
    connection = sqlite3.connect(":memory:")
    statements: List[str] = [
        "CREATE TABLE example (id INTEGER PRIMARY KEY, value TEXT);",
        "INSERT INTO example (value) VALUES ('alpha');",
    ]

    setup_database.apply_schema(connection, statements)

    rows = connection.execute("SELECT value FROM example").fetchall()
    assert rows == [("alpha",)]


def test_create_schema_with_explicit_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = tmp_path / "nfl.sqlite"
    assert not db_path.exists()

    created_path = setup_database.create_schema(db_path)

    assert created_path == db_path
    assert db_path.exists()
    with sqlite3.connect(db_path) as conn:
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='match_predictions';"
        ).fetchall()
        assert tables == [("match_predictions",)]


def test_create_schema_uses_get_database_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = tmp_path / "auto.db"

    def fake_get_database_path() -> Path:
        return db_path

    called = {"apply": 0, "get_path": 0}

    def fake_apply_schema(connection: sqlite3.Connection, statements=setup_database.SCHEMA_STATEMENTS) -> None:
        called["apply"] += 1
        assert statements == setup_database.SCHEMA_STATEMENTS

    monkeypatch.setattr(setup_database, "get_database_path", fake_get_database_path)
    monkeypatch.setattr(setup_database, "apply_schema", fake_apply_schema)

    result_path = setup_database.create_schema()

    assert result_path == db_path
    assert called["apply"] == 1


# sync 1774962760781500803
# sync 1774962763835833745
# sync 1774962786416675584
# sync 1774962859112288837
# sys_sync_30af42f5
# sys_sync_1b54f3b
# sys_sync_8ba4879
# sys_sync_32647d38
# sys_sync_52a62423
# sys_sync_74d02e12
