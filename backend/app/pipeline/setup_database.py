"""
Database schema management utilities for the NFL prediction pipeline.

This module creates the SQLite schema expected by the pipeline, scheduler,
and API layers. It can be executed as a script or imported and invoked from
other modules (e.g., tests or setup scripts).
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Iterable, Sequence

from dotenv import load_dotenv

load_dotenv()

# Absolute default: backend/data/database/nfl_predictions.db
BACKEND_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DB_PATH = BACKEND_ROOT / "data" / "database" / "nfl_predictions.db"


SCHEMA_STATEMENTS: Sequence[str] = (
    "DROP TABLE IF EXISTS match_predictions;",
    "DROP TABLE IF EXISTS teams;",
    "DROP TABLE IF EXISTS matches;",
    "DROP TABLE IF EXISTS predictions;",
    "DROP TABLE IF EXISTS results;",
    """
    CREATE TABLE IF NOT EXISTS match_predictions (
        game_id TEXT PRIMARY KEY,
        year INTEGER NOT NULL,
        week INTEGER NOT NULL,
        home_team TEXT NOT NULL,
        away_team TEXT NOT NULL,
        home_team_win_probability REAL NOT NULL,
        away_team_win_probability REAL NOT NULL,
        predicted_result TEXT NOT NULL,
        home_team_image_url TEXT,
        away_team_image_url TEXT,
        home_coach TEXT,
        away_coach TEXT,
        stadium TEXT,
        home_score INTEGER,
        away_score INTEGER
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_match_predictions_year_week
        ON match_predictions (year, week);
    """,
)


def get_database_path() -> Path:
    """
    Resolve the database path from the environment or fallback to defaults.

    Returns:
        Path: Absolute path to the SQLite database file.
    """
    configured_path = os.getenv("database_path")
    if configured_path:
        resolved = Path(configured_path)
        if not resolved.is_absolute():
            resolved = BACKEND_ROOT / resolved
        return resolved
    return DEFAULT_DB_PATH


def apply_schema(connection: sqlite3.Connection, statements: Iterable[str] = SCHEMA_STATEMENTS) -> None:
    """
    Apply each SQL statement to the provided database connection.

    Args:
        connection: Active sqlite3 connection.
        statements: Iterable of SQL statements to execute sequentially.
    """
    cursor = connection.cursor()
    for statement in statements:
        cursor.executescript(statement)
    connection.commit()


def create_schema(database_path: Path | None = None) -> Path:
    """
    Create the database schema if it does not exist.

    Args:
        database_path: Optional explicit database path override.

    Returns:
        Path: The path to the database file that was initialized.
    """
    db_path = database_path or get_database_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as connection:
        connection.execute("PRAGMA foreign_keys = ON;")
        apply_schema(connection)

    return db_path


if __name__ == "__main__":
    path = create_schema()
    print(f"✅ Database schema ensured at: {path}")


# sys_sync_70d6626c
# sys_sync_27af1ff1
# sys_sync_a5f2260
# sys_sync_7ee42b6f
# sys_sync_1e4c318
# sys_sync_6cf33cc0
