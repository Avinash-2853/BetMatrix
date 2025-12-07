# type: ignore
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
import pandas as pd
import pytest
from app.pipeline.clean import (
    remove_columns,
    aggregate_categorical_counts,
    aggregate_match_features_with_nulls,
    aggregate_positive_negative,
)

# --------------------
# Fixtures / helpers
# --------------------
def make_sample_df():
    return pd.DataFrame({
        "game_id": [1, 1, 1, 2, 2],
        "posteam": ["A", "A", "B", "C", "D"],
        "home_team": ["A", "A", "A", "C", "C"],
        "away_team": ["B", "B", "B", "D", "D"],
        "game_seconds_remaining": [100, 0, 0, 50, 0],
        "yards": [10, -5, 0, -2, 15],
        "field_goal_result": ["made", "missed", "blocked", "made", "missed"],
        "extra_point_result": ["good", "failed", "blocked", "good", "aborted"],
        "first_down_rush": [1, 0, 2, 3, 1],
        "first_down_pass": [0, 1, 1, 0, 1],
    })


# --------------------
# Tests for remove_columns
# --------------------
def test_remove_columns_basic():
    df = make_sample_df()
    result = remove_columns(df, ["yards"])
    assert "yards" not in result.columns
    assert "game_id" in result.columns
    # Original df unchanged
    assert "yards" in df.columns


def test_remove_columns_keyerror():
    df = make_sample_df()
    with pytest.raises(KeyError):
        _ = remove_columns(df, ["not_a_column"])


# --------------------
# Tests for aggregate_categorical_counts
# --------------------
def test_aggregate_categorical_counts_basic():
    df = make_sample_df()
    result = aggregate_categorical_counts(
        df, cat_cols=["field_goal_result", "extra_point_result"]
    )

    # Check some expected columns
    assert "home_field_goal_result_made" in result.columns
    assert "away_field_goal_result_blocked" in result.columns
    assert "home_extra_point_result_good" in result.columns
    assert "away_extra_point_result_aborted" in result.columns

    # Non-last rows should be NaN for new cols
    non_last = result[result["game_seconds_remaining"] != 0]
    new_cols = [c for c in result.columns if "field_goal_result" in c or "extra_point_result" in c]
    assert non_last[new_cols].isna().all().all()


def test_aggregate_categorical_counts_multiple_games():
    df = make_sample_df()
    result = aggregate_categorical_counts(df, cat_cols=["field_goal_result"])
    # Ensure counts are restricted to last rows
    last_rows = result[result["game_seconds_remaining"] == 0]
    assert last_rows.filter(like="field_goal_result").notna().any().any()


# --------------------
# Tests for aggregate_match_features_with_nulls
# --------------------
def test_aggregate_match_features_with_nulls_basic():
    df = make_sample_df()
    result = aggregate_match_features_with_nulls(
        df,
        stat_cols=["first_down_rush", "first_down_pass"]
    )

    assert "home_first_down_rush" in result.columns
    assert "away_first_down_pass" in result.columns

    # Only last rows should have values
    non_last = result[result["game_seconds_remaining"] != 0]
    new_cols = ["home_first_down_rush", "home_first_down_pass",
                "away_first_down_rush", "away_first_down_pass"]
    assert non_last[new_cols].isna().all().all()

    # Last rows must have numbers
    last_rows = result[result["game_seconds_remaining"] == 0]
    assert last_rows[new_cols].notna().any().any()


def test_aggregate_match_features_with_nulls_valueerror():
    df = make_sample_df()
    with pytest.raises(ValueError):
        _ = aggregate_match_features_with_nulls(df, stat_cols=None)


# --------------------
# Tests for aggregate_positive_negative
# --------------------
def test_aggregate_positive_negative_basic():
    df = make_sample_df()
    result = aggregate_positive_negative(df, num_cols=["yards"])

    expected_cols = [
        "home_yards_positive", "home_yards_negative",
        "away_yards_positive", "away_yards_negative"
    ]
    for col in expected_cols:
        assert col in result.columns

    # Non-last rows -> NaN in new cols
    non_last = result[result["game_seconds_remaining"] != 0]
    assert non_last[expected_cols].isna().all().all()

    # Last rows -> not all NaN
    last_rows = result[result["game_seconds_remaining"] == 0]
    assert last_rows[expected_cols].notna().any().any()


def test_aggregate_positive_negative_all_positive():
    df = make_sample_df()
    df["yards"] = [5, 10, 20, 1, 2]  # all positive
    result = aggregate_positive_negative(df, num_cols=["yards"])
    last_rows = result[result["game_seconds_remaining"] == 0]
    assert (last_rows["home_yards_negative"].fillna(0) == 0).all()


def test_aggregate_positive_negative_all_negative():
    df = make_sample_df()
    df["yards"] = [-5, -10, -20, -1, -2]  # all negative
    result = aggregate_positive_negative(df, num_cols=["yards"])
    last_rows = result[result["game_seconds_remaining"] == 0]
    assert (last_rows["home_yards_positive"].fillna(0) == 0).all()
# sync 1774962714447953116
# sys_sync_326d2e00
# sys_sync_53006ea3
# sys_sync_450b4da1
