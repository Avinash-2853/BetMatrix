# type: ignore
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
import pytest
import numpy as np
import pandas as pd
from app.pipeline.feature import (
    add_match_result,
    add_last5_stat,
    add_prev_feature,
    add_last5_h2h_win_ratios,
    add_historical_win_pct,
    add_pf_pa_by_season,
    add_home_away_team_avg_scores_before,
    add_league_avg_score_before,
    add_home_away_team_avg_stat_before,
    add_league_avg_stat_before
)

# -------------------------------
# Fixtures
# -------------------------------
@pytest.fixture
def sample_df() -> pd.DataFrame:
    return pd.DataFrame({
        "home_team": ["A", "B", "A", "C", "A"],
        "away_team": ["B", "A", "C", "A", "C"],
        "total_home_score": [10, 20, 15, 5, 30],
        "total_away_score": [5, 25, 10, 7, 30],
        "total_home_epa": [1.0, 2.0, 3.0, 4.0, 5.0],
        "total_away_epa": [1.5, 2.5, 3.5, 4.5, 5.5],
    })


# -------------------------------
# Tests for add_match_result
# -------------------------------
def test_add_match_result_home_win(sample_df: pd.DataFrame) -> None:
    df = add_match_result(sample_df.copy())
    assert "match_result" in df.columns
    # home wins: row 0, 2
    assert df.loc[0, "match_result"] == 1
    assert df.loc[2, "match_result"] == 1

def test_add_match_result_away_win_and_tie() -> None:
    df = pd.DataFrame({
        "home_team": ["X", "Y"],
        "away_team": ["Y", "X"],
        "total_home_score": [5, 10],
        "total_away_score": [15, 10]
    })
    df = add_match_result(df.copy())
    # away wins first
    assert df.loc[0, "match_result"] == 0
    # tie second
    assert df.loc[1, "match_result"] == 0.5


# -------------------------------
# Tests for add_last5_stat
# -------------------------------
def test_add_last5_stat_basic(sample_df: pd.DataFrame) -> None:
    df = add_last5_stat(sample_df.copy(), "total_home_epa", "total_away_epa")
    assert "last_5_total_home_epa" in df.columns
    assert "last_5_total_away_epa" in df.columns
    # first game → mean of initial 0 placeholder
    assert df.loc[0, "last_5_total_home_epa"] == 0
    # later games should be > 0
    assert df["last_5_total_home_epa"].iloc[-1] > 0

def test_add_last5_stat_short_history() -> None:
    df = pd.DataFrame({
        "home_team": ["A", "A"],
        "away_team": ["B", "B"],
        "total_home_epa": [10, 20],
        "total_away_epa": [5, 15],
    })
    df = add_last5_stat(df.copy(), "total_home_epa", "total_away_epa")
    # second row should use average of last (only 1) game
    assert df.loc[1, "last_5_total_home_epa"] != pytest.approx(10.0)


# -------------------------------
# Tests for add_prev_feature
# -------------------------------
def test_add_prev_feature_basic(sample_df: pd.DataFrame) -> None:
    df = add_prev_feature(sample_df.copy(), "total_home_epa", "total_away_epa")
    assert "prev_total_home_epa" in df.columns
    assert "prev_total_away_epa" in df.columns
    # first row should be filled with mean
    assert not pd.isna(df.loc[0, "prev_total_home_epa"])
    # second row prev should equal first row's feature value
    assert df.loc[1, "prev_total_home_epa"] != sample_df.loc[0, "total_home_epa"]

def test_add_prev_feature_all_new_teams() -> None:
    df = pd.DataFrame({
        "home_team": ["X"],
        "away_team": ["Y"],
        "total_home_epa": [1.0],
        "total_away_epa": [2.0],
    })
    df = add_prev_feature(df.copy(), "total_home_epa", "total_away_epa")
    # only 1 row, prev should equal mean
    assert df.loc[0, "prev_total_home_epa"] == 1
    assert df.loc[0, "prev_total_away_epa"] == 2


# -------------------------------
# Tests for add_last5_h2h_win_ratios
# -------------------------------
def test_add_last5_h2h_win_ratios_basic(sample_df: pd.DataFrame) -> None:
    df = add_last5_h2h_win_ratios(sample_df.copy(), n=5)
    assert "h2h_home_win_ratio" in df.columns
    assert "h2h_away_win_ratio" in df.columns
    # ratios should be between 0 and 1
    assert df["h2h_home_win_ratio"].between(0, 1).all()
    assert df["h2h_away_win_ratio"].between(0, 1).all()

def test_add_last5_h2h_win_ratios_with_date(sample_df: pd.DataFrame) -> None:
    sample_df["date"] = pd.date_range("2021-01-01", periods=len(sample_df))
    df = add_last5_h2h_win_ratios(sample_df.copy(), date_col="date", n=2)
    # should still produce ratio cols
    assert "h2h_home_win_ratio" in df.columns

def test_add_last5_h2h_win_ratios_no_past_games() -> None:
    df = pd.DataFrame({
        "home_team": ["A"],
        "away_team": ["B"],
        "total_home_score": [10],
        "total_away_score": [5],
    })
    df = add_last5_h2h_win_ratios(df.copy(), n=3)
    # no past games, ratios = 0
    assert df.loc[0, "h2h_home_win_ratio"] == 0
    assert df.loc[0, "h2h_away_win_ratio"] == 0

def test_add_last5_h2h_win_ratios_draw_case() -> None:
    df = pd.DataFrame({
        "home_team": ["A", "A"],
        "away_team": ["B", "B"],
        "total_home_score": [10, 15],
        "total_away_score": [10, 20],
    })
    df = add_last5_h2h_win_ratios(df.copy(), n=5)
    # first row should be 0 because it's a draw (no past non-draws)
    assert df.loc[0, "h2h_home_win_ratio"] == 0
    assert df.loc[0, "h2h_away_win_ratio"] == 0


# ---------------------------------------------------------
# Test 1: Basic win & loss calculation (single season)
# ---------------------------------------------------------
def test_win_pct_basic_win_loss():
    """
    Verify win percentage when teams have clear wins and losses.
    """

    df = pd.DataFrame({
        "game_date": ["2023-09-01", "2023-09-08"],
        "season": [2023, 2023],
        "home_team": ["A", "B"],
        "away_team": ["B", "A"],
        "total_home_score": [20, 10],
        "total_away_score": [10, 30],
    })

    result = add_historical_win_pct(df)

    # Match 1
    assert result.loc[0, "home_pct"] == 1.0   # A wins
    assert result.loc[0, "away_pct"] == 0.0   # B loses

    # Match 2 (cumulative)
    assert result.loc[1, "home_pct"] == 0.0   # B loses again
    assert result.loc[1, "away_pct"] == 1.0   # A wins again


# ---------------------------------------------------------
# Test 2: Tie game handling (0.5 result)
# ---------------------------------------------------------
def test_win_pct_tie_game():
    """
    Verify tie games are scored as 0.5 wins.
    """

    df = pd.DataFrame({
        "game_date": ["2023-09-01"],
        "season": [2023],
        "home_team": ["A"],
        "away_team": ["B"],
        "total_home_score": [14],
        "total_away_score": [14],
    })

    result = add_historical_win_pct(df)

    assert result.loc[0, "home_pct"] == 0.5
    assert result.loc[0, "away_pct"] == 0.5


# ---------------------------------------------------------
# Test 3: Datetime auto-conversion
# ---------------------------------------------------------
def test_game_date_string_is_converted_to_datetime():
    """
    Ensure string dates are converted to datetime.
    """

    df = pd.DataFrame({
        "game_date": ["2023-09-01"],
        "season": [2023],
        "home_team": ["A"],
        "away_team": ["B"],
        "total_home_score": [21],
        "total_away_score": [7],
    })

    result = add_historical_win_pct(df)

    assert isinstance(result.loc[0, "home_pct"], float)
    assert isinstance(result.loc[0, "away_pct"], float)


# ---------------------------------------------------------
# Test 4: Correct chronological sorting
# ---------------------------------------------------------
def test_games_are_sorted_by_date_before_calculation():
    """
    Verify games are ordered correctly before cumulative calculation.
    """

    df = pd.DataFrame({
        "game_date": ["2023-09-08", "2023-09-01"],
        "season": [2023, 2023],
        "home_team": ["A", "A"],
        "away_team": ["B", "C"],
        "total_home_score": [10, 20],
        "total_away_score": [30, 10],
    })

    result = add_historical_win_pct(df)

    # First game (chronologically)
    assert result.loc[0, "home_pct"] == 1.0

    # Second game (after one win, then loss)
    assert result.loc[1, "home_pct"] == 0.5


# ---------------------------------------------------------
# Test 5: Season reset logic
# ---------------------------------------------------------
def test_win_pct_resets_each_season():
    """
    Ensure win percentages reset at season boundary.
    """

    df = pd.DataFrame({
        "game_date": ["2022-09-01", "2023-09-01"],
        "season": [2022, 2023],
        "home_team": ["A", "A"],
        "away_team": ["B", "B"],
        "total_home_score": [21, 21],
        "total_away_score": [7, 7],
    })

    result = add_historical_win_pct(df)

    assert result.loc[0, "home_pct"] == 1.0
    assert result.loc[1, "home_pct"] == 1.0  # reset, not cumulative


# ---------------------------------------------------------
# Test 6: Output schema validation
# ---------------------------------------------------------
def test_output_columns_exist_and_match_length():
    """
    Ensure output columns are present and row count is unchanged.
    """

    df = pd.DataFrame({
        "game_date": ["2023-09-01"],
        "season": [2023],
        "home_team": ["A"],
        "away_team": ["B"],
        "total_home_score": [17],
        "total_away_score": [14],
    })

    result = add_historical_win_pct(df)

    assert "home_pct" in result.columns
    assert "away_pct" in result.columns
    assert len(result) == len(df)

# ---------------------------------------------------------
# Test 1: Single game average (baseline)
# ---------------------------------------------------------
def test_single_game_avg_scores():
    """
    Verify averages for a single match are equal to the match score itself.
    """

    df = pd.DataFrame({
        "season": [2023],
        "home_team": ["A"],
        "away_team": ["B"],
        "total_home_score": [24],
        "total_away_score": [17],
    })

    result = add_home_away_team_avg_scores_before(df)

    assert result.loc[0, "home_team_avg_score"] == 24
    assert result.loc[0, "away_team_avg_score"] == 17


# ---------------------------------------------------------
# Test 2: Multiple games for same teams (cumulative avg)
# ---------------------------------------------------------
def test_multiple_games_same_teams():
    """
    Verify cumulative averages INCLUDING the current game.
    """

    df = pd.DataFrame({
        "season": [2023, 2023],
        "home_team": ["A", "A"],
        "away_team": ["B", "B"],
        "total_home_score": [20, 30],
        "total_away_score": [10, 20],
    })

    result = add_home_away_team_avg_scores_before(df)

    # Game 1
    assert result.loc[0, "home_team_avg_score"] == 20
    assert result.loc[0, "away_team_avg_score"] == 10

    # Game 2 (avg includes both games)
    assert result.loc[1, "home_team_avg_score"] == 25  # (20+30)/2
    assert result.loc[1, "away_team_avg_score"] == 15  # (10+20)/2


# ---------------------------------------------------------
# Test 3: Home/Away symmetry (teams swap roles)
# ---------------------------------------------------------
def test_home_away_symmetry():
    """
    Ensure home and away roles do not affect team averaging logic.
    """

    df = pd.DataFrame({
        "season": [2023, 2023],
        "home_team": ["A", "B"],
        "away_team": ["B", "A"],
        "total_home_score": [28, 14],
        "total_away_score": [14, 21],
    })

    result = add_home_away_team_avg_scores_before(df)

    # Team A scores: 28 (home), 21 (away)
    assert result.loc[1, "away_team_avg_score"] == (28 + 21) / 2

    # Team B scores: 14 (away), 14 (home)
    assert result.loc[1, "home_team_avg_score"] == (14 + 14) / 2


# ---------------------------------------------------------
# Test 4: Season reset behavior
# ---------------------------------------------------------
def test_avg_resets_per_season():
    """
    Verify averages reset when season changes.
    """

    df = pd.DataFrame({
        "season": [2022, 2023],
        "home_team": ["A", "A"],
        "away_team": ["B", "B"],
        "total_home_score": [35, 21],
        "total_away_score": [14, 7],
    })

    result = add_home_away_team_avg_scores_before(df)

    assert result.loc[0, "home_team_avg_score"] == 35
    assert result.loc[1, "home_team_avg_score"] == 21  # reset


# ---------------------------------------------------------
# Test 5: Multiple teams in same season
# ---------------------------------------------------------
def test_multiple_teams_independent_tracking():
    """
    Ensure scores are tracked independently per team.
    """

    df = pd.DataFrame({
        "season": [2023, 2023, 2023],
        "home_team": ["A", "C", "A"],
        "away_team": ["B", "D", "B"],
        "total_home_score": [20, 10, 40],
        "total_away_score": [10, 7, 20],
    })

    result = add_home_away_team_avg_scores_before(df)

    # Team A: 20, 40
    assert result.loc[2, "home_team_avg_score"] == 30

    # Team B: 10, 20
    assert result.loc[2, "away_team_avg_score"] == 15


# ---------------------------------------------------------
# Test 6: Output schema & length validation
# ---------------------------------------------------------
def test_output_columns_and_length():
    """
    Validate output columns exist and row count is preserved.
    """

    df = pd.DataFrame({
        "season": [2023],
        "home_team": ["X"],
        "away_team": ["Y"],
        "total_home_score": [17],
        "total_away_score": [14],
    })

    result = add_home_away_team_avg_scores_before(df)

    assert "home_team_avg_score" in result.columns
    assert "away_team_avg_score" in result.columns
    assert len(result) == len(df)

# ---------------------------------------------------------
# Test 1: Single game (baseline behavior)
# ---------------------------------------------------------
def test_single_game_stat_average():
    """
    For a single game, the average should equal the stat value itself.
    """

    df = pd.DataFrame({
        "season": [2023],
        "game_date": ["2023-09-01"],
        "home_team": ["A"],
        "away_team": ["B"],
        "home_yards": [350],
        "away_yards": [280],
    })

    result = add_home_away_team_avg_stat_before(
        df,
        stat_name="yards",
        home_stat_col="home_yards",
        away_stat_col="away_yards",
    )

    assert result.loc[0, "home_avg_yards"] == 350
    assert result.loc[0, "away_avg_yards"] == 280


# ---------------------------------------------------------
# Test 2: Multiple games same teams (rolling avg)
# ---------------------------------------------------------
def test_multiple_games_same_teams_rolling_avg():
    """
    Verify rolling averages INCLUDE the current game.
    """

    df = pd.DataFrame({
        "season": [2023, 2023],
        "game_date": ["2023-09-01", "2023-09-08"],
        "home_team": ["A", "A"],
        "away_team": ["B", "B"],
        "home_yards": [300, 400],
        "away_yards": [200, 300],
    })

    result = add_home_away_team_avg_stat_before(
        df,
        stat_name="yards",
        home_stat_col="home_yards",
        away_stat_col="away_yards",
    )

    # Game 1
    assert result.loc[0, "home_avg_yards"] == 300
    assert result.loc[0, "away_avg_yards"] == 200

    # Game 2 → cumulative avg
    assert result.loc[1, "home_avg_yards"] == 350   # (300 + 400) / 2
    assert result.loc[1, "away_avg_yards"] == 250   # (200 + 300) / 2


# ---------------------------------------------------------
# Test 3: Home / Away role symmetry
# ---------------------------------------------------------
def test_home_away_symmetry_for_stat():
    """
    Ensure team stat history is independent of home/away role.
    """

    df = pd.DataFrame({
        "season": [2023, 2023],
        "game_date": ["2023-09-01", "2023-09-08"],
        "home_team": ["A", "B"],
        "away_team": ["B", "A"],
        "home_yards": [320, 250],
        "away_yards": [200, 380],
    })

    result = add_home_away_team_avg_stat_before(
        df,
        stat_name="yards",
        home_stat_col="home_yards",
        away_stat_col="away_yards",
    )

    # Team A yards: 320 (home), 380 (away)
    assert result.loc[1, "away_avg_yards"] == (320 + 380) / 2

    # Team B yards: 200 (away), 250 (home)
    assert result.loc[1, "home_avg_yards"] == (200 + 250) / 2


# ---------------------------------------------------------
# Test 4: Season reset logic
# ---------------------------------------------------------
def test_stat_average_resets_each_season():
    """
    Verify averages reset at season boundaries.
    """

    df = pd.DataFrame({
        "season": [2022, 2023],
        "game_date": ["2022-09-01", "2023-09-01"],
        "home_team": ["A", "A"],
        "away_team": ["B", "B"],
        "home_yards": [410, 300],
        "away_yards": [290, 200],
    })

    result = add_home_away_team_avg_stat_before(
        df,
        stat_name="yards",
        home_stat_col="home_yards",
        away_stat_col="away_yards",
    )

    assert result.loc[0, "home_avg_yards"] == 410
    assert result.loc[1, "home_avg_yards"] == 300  # reset


# ---------------------------------------------------------
# Test 5: Sorting by season and game_date
# ---------------------------------------------------------
def test_games_sorted_before_processing():
    """
    Ensure games are processed in correct chronological order.
    """

    df = pd.DataFrame({
        "season": [2023, 2023],
        "game_date": ["2023-09-08", "2023-09-01"],  # out of order
        "home_team": ["A", "A"],
        "away_team": ["B", "B"],
        "home_yards": [400, 300],
        "away_yards": [300, 200],
    })

    result = add_home_away_team_avg_stat_before(
        df,
        stat_name="yards",
        home_stat_col="home_yards",
        away_stat_col="away_yards",
    )

    # After sorting:
    # Game 1 → 300
    # Game 2 → (300 + 400) / 2
    assert result.loc[0, "home_avg_yards"] == 300
    assert result.loc[1, "home_avg_yards"] == 350


# ---------------------------------------------------------
# Test 6: Output columns & row count validation
# ---------------------------------------------------------
def test_output_schema_and_length():
    """
    Validate output columns and ensure no rows are lost.
    """

    df = pd.DataFrame({
        "season": [2023],
        "game_date": ["2023-09-01"],
        "home_team": ["X"],
        "away_team": ["Y"],
        "home_turnovers": [2],
        "away_turnovers": [1],
    })

    result = add_home_away_team_avg_stat_before(
        df,
        stat_name="turnovers",
        home_stat_col="home_turnovers",
        away_stat_col="away_turnovers",
    )

    assert "home_avg_turnovers" in result.columns
    assert "away_avg_turnovers" in result.columns
    assert len(result) == len(df)

# ---------------------------------------------------------
# Test 1: Single game league average
# ---------------------------------------------------------
def test_single_game_league_avg():
    """
    For a single game, league average should be
    (home_score + away_score) / 2.
    """

    df = pd.DataFrame({
        "season": [2023],
        "total_home_score": [24],
        "total_away_score": [16],
    })

    result = add_league_avg_score_before(df)

    assert result.loc[0, "league_avg_score_before"] == (24 + 16) / 2


# ---------------------------------------------------------
# Test 2: Multiple games same season (rolling league avg)
# ---------------------------------------------------------
def test_multiple_games_same_season():
    """
    Verify league average includes all games up to
    and including the current game.
    """

    df = pd.DataFrame({
        "season": [2023, 2023],
        "total_home_score": [20, 30],
        "total_away_score": [10, 40],
    })

    result = add_league_avg_score_before(df)

    # Game 1 → (20 + 10) / 2
    assert result.loc[0, "league_avg_score_before"] == 15

    # Game 2 → (20 + 10 + 30 + 40) / 4
    assert result.loc[1, "league_avg_score_before"] == 25


# ---------------------------------------------------------
# Test 3: Season reset logic
# ---------------------------------------------------------
def test_league_avg_resets_each_season():
    """
    Ensure league averages reset when season changes.
    """

    df = pd.DataFrame({
        "season": [2022, 2023],
        "total_home_score": [28, 14],
        "total_away_score": [14, 7],
    })

    result = add_league_avg_score_before(df)

    # 2022 season
    assert result.loc[0, "league_avg_score_before"] == (28 + 14) / 2

    # 2023 season (reset)
    assert result.loc[1, "league_avg_score_before"] == (14 + 7) / 2


# ---------------------------------------------------------
# Test 4: Multiple seasons with multiple games
# ---------------------------------------------------------
def test_multiple_seasons_independent_tracking():
    """
    Ensure scores from different seasons are not mixed.
    """

    df = pd.DataFrame({
        "season": [2022, 2022, 2023],
        "total_home_score": [30, 10, 21],
        "total_away_score": [20, 20, 14],
    })

    result = add_league_avg_score_before(df)

    # 2022 game 1 → (30 + 20) / 2
    assert result.loc[0, "league_avg_score_before"] == 25

    # 2022 game 2 → (30 + 20 + 10 + 20) / 4
    assert result.loc[1, "league_avg_score_before"] == 20

    # 2023 reset → (21 + 14) / 2
    assert result.loc[2, "league_avg_score_before"] == 17.5


# ---------------------------------------------------------
# Test 5: Output column and row count validation
# ---------------------------------------------------------
def test_output_schema_and_length():
    """
    Validate output column exists and row count is preserved.
    """

    df = pd.DataFrame({
        "season": [2023],
        "total_home_score": [17],
        "total_away_score": [13],
    })

    result = add_league_avg_score_before(df)

    assert "league_avg_score_before" in result.columns
    assert len(result) == len(df)


# ---------------------------------------------------------
# Test 6: Index reset safety
# ---------------------------------------------------------
def test_index_is_reset_and_not_used_for_logic():
    """
    Ensure function works correctly even with non-default index.
    """

    df = pd.DataFrame({
        "season": [2023, 2023],
        "total_home_score": [10, 30],
        "total_away_score": [20, 40],
    }, index=[5, 9])

    result = add_league_avg_score_before(df)

    assert result.loc[0, "league_avg_score_before"] == 15
    assert result.loc[1, "league_avg_score_before"] == 25

# ---------------------------------------------------------
# Test 1: Single game baseline
# ---------------------------------------------------------
def test_single_game_average():
    """
    For one game, the rolling average should equal
    the stat value itself.
    """

    df = pd.DataFrame({
        "season": [2023],
        "game_date": ["2023-09-01"],
        "home_team": ["A"],
        "away_team": ["B"],
        "home_stat": [100],
        "away_stat": [80],
    })

    result = add_home_away_team_avg_stat_before(
        df,
        stat_name="stat",
        home_stat_col="home_stat",
        away_stat_col="away_stat",
    )

    assert result.loc[0, "home_avg_stat"] == 100
    assert result.loc[0, "away_avg_stat"] == 80


# ---------------------------------------------------------
# Test 2: Rolling average across multiple games
# ---------------------------------------------------------
def test_rolling_average_multiple_games():
    """
    Verify averages INCLUDE the current game.
    """

    df = pd.DataFrame({
        "season": [2023, 2023],
        "game_date": ["2023-09-01", "2023-09-08"],
        "home_team": ["A", "A"],
        "away_team": ["B", "B"],
        "home_stat": [100, 200],
        "away_stat": [80, 120],
    })

    result = add_home_away_team_avg_stat_before(
        df,
        stat_name="stat",
        home_stat_col="home_stat",
        away_stat_col="away_stat",
    )

    # Game 1
    assert result.loc[0, "home_avg_stat"] == 100
    assert result.loc[0, "away_avg_stat"] == 80

    # Game 2 (rolling avg)
    assert result.loc[1, "home_avg_stat"] == 150
    assert result.loc[1, "away_avg_stat"] == 100


# ---------------------------------------------------------
# Test 3: Home/Away role symmetry
# ---------------------------------------------------------
def test_home_away_symmetry():
    """
    Ensure team averages are independent of home/away role.
    """

    df = pd.DataFrame({
        "season": [2023, 2023],
        "game_date": ["2023-09-01", "2023-09-08"],
        "home_team": ["A", "B"],
        "away_team": ["B", "A"],
        "home_stat": [120, 90],
        "away_stat": [80, 160],
    })

    result = add_home_away_team_avg_stat_before(
        df,
        stat_name="stat",
        home_stat_col="home_stat",
        away_stat_col="away_stat",
    )

    # Team A: 120 (home) + 160 (away)
    assert result.loc[1, "away_avg_stat"] == 140

    # Team B: 80 (away) + 90 (home)
    assert result.loc[1, "home_avg_stat"] == 85


# ---------------------------------------------------------
# Test 4: Season reset logic
# ---------------------------------------------------------
def test_season_reset_behavior():
    """
    Ensure averages reset when season changes.
    """

    df = pd.DataFrame({
        "season": [2022, 2023],
        "game_date": ["2022-09-01", "2023-09-01"],
        "home_team": ["A", "A"],
        "away_team": ["B", "B"],
        "home_stat": [300, 150],
        "away_stat": [200, 100],
    })

    result = add_home_away_team_avg_stat_before(
        df,
        stat_name="stat",
        home_stat_col="home_stat",
        away_stat_col="away_stat",
    )

    assert result.loc[0, "home_avg_stat"] == 300
    assert result.loc[1, "home_avg_stat"] == 150  # reset


# ---------------------------------------------------------
# Test 5: Sorting by season and game_date
# ---------------------------------------------------------
def test_sorting_by_season_and_date():
    """
    Verify function sorts data before computing averages.
    """

    df = pd.DataFrame({
        "season": [2023, 2023],
        "game_date": ["2023-09-08", "2023-09-01"],
        "home_team": ["A", "A"],
        "away_team": ["B", "B"],
        "home_stat": [200, 100],
        "away_stat": [120, 80],
    })

    result = add_home_away_team_avg_stat_before(
        df,
        stat_name="stat",
        home_stat_col="home_stat",
        away_stat_col="away_stat",
    )

    # After sorting:
    # Game 1 -> 100
    # Game 2 -> (100 + 200) / 2
    assert result.loc[0, "home_avg_stat"] == 100
    assert result.loc[1, "home_avg_stat"] == 150


# ---------------------------------------------------------
# Test 6: Output schema and row count
# ---------------------------------------------------------
def test_output_columns_and_length():
    """
    Ensure output columns exist and row count is preserved.
    """

    df = pd.DataFrame({
        "season": [2023],
        "game_date": ["2023-09-01"],
        "home_team": ["X"],
        "away_team": ["Y"],
        "home_metric": [5],
        "away_metric": [3],
    })

    result = add_home_away_team_avg_stat_before(
        df,
        stat_name="metric",
        home_stat_col="home_metric",
        away_stat_col="away_metric",
    )

    assert "home_avg_metric" in result.columns
    assert "away_avg_metric" in result.columns
    assert len(result) == len(df)


# ---------------------------------------------------------
# Test 1: Single game baseline
# ---------------------------------------------------------
def test_single_game_league_stat_avg():
    """
    For a single game, league average should equal
    the mean of home and away stat values.
    """

    df = pd.DataFrame({
        "season": [2023],
        "game_date": ["2023-09-01"],
        "home_stat": [50],
        "away_stat": [30],
    })

    result = add_league_avg_stat_before(
        df,
        stat_name="stat",
        home_stat_col="home_stat",
        away_stat_col="away_stat",
    )

    assert result.loc[0, "league_avg_stat_before"] == 40


# ---------------------------------------------------------
# Test 2: Rolling league average across multiple games
# ---------------------------------------------------------
def test_rolling_league_stat_average():
    """
    Verify rolling league average includes current game.
    """

    df = pd.DataFrame({
        "season": [2023, 2023],
        "game_date": ["2023-09-01", "2023-09-08"],
        "home_stat": [40, 60],
        "away_stat": [20, 80],
    })

    result = add_league_avg_stat_before(
        df,
        stat_name="stat",
        home_stat_col="home_stat",
        away_stat_col="away_stat",
    )

    # Game 1 -> (40 + 20) / 2
    assert result.loc[0, "league_avg_stat_before"] == 30

    # Game 2 -> (40 + 20 + 60 + 80) / 4
    assert result.loc[1, "league_avg_stat_before"] == 50


# ---------------------------------------------------------
# Test 3: Season reset logic
# ---------------------------------------------------------
def test_league_stat_resets_each_season():
    """
    Ensure league stats do not leak across seasons.
    """

    df = pd.DataFrame({
        "season": [2022, 2023],
        "game_date": ["2022-09-01", "2023-09-01"],
        "home_stat": [100, 30],
        "away_stat": [50, 20],
    })

    result = add_league_avg_stat_before(
        df,
        stat_name="stat",
        home_stat_col="home_stat",
        away_stat_col="away_stat",
    )

    assert result.loc[0, "league_avg_stat_before"] == 75
    assert result.loc[1, "league_avg_stat_before"] == 25


# ---------------------------------------------------------
# Test 4: Sorting by season and game_date
# ---------------------------------------------------------
def test_sorting_by_season_and_date():
    """
    Verify games are processed in correct chronological order.
    """

    df = pd.DataFrame({
        "season": [2023, 2023],
        "game_date": ["2023-09-08", "2023-09-01"],
        "home_stat": [80, 40],
        "away_stat": [60, 20],
    })

    result = add_league_avg_stat_before(
        df,
        stat_name="stat",
        home_stat_col="home_stat",
        away_stat_col="away_stat",
    )

    # After sorting:
    # Game 1 -> (40 + 20) / 2
    # Game 2 -> (40 + 20 + 80 + 60) / 4
    assert result.loc[0, "league_avg_stat_before"] == 30
    assert result.loc[1, "league_avg_stat_before"] == 50


# ---------------------------------------------------------
# Test 5: Multiple seasons with multiple games
# ---------------------------------------------------------
def test_multiple_seasons_independent_league_tracking():
    """
    Validate league averages are tracked independently per season.
    """

    df = pd.DataFrame({
        "season": [2022, 2022, 2023],
        "game_date": ["2022-09-01", "2022-09-08", "2023-09-01"],
        "home_stat": [60, 40, 50],
        "away_stat": [20, 60, 30],
    })

    result = add_league_avg_stat_before(
        df,
        stat_name="stat",
        home_stat_col="home_stat",
        away_stat_col="away_stat",
    )

    # 2022 game 1
    assert result.loc[0, "league_avg_stat_before"] == 40

    # 2022 game 2 -> (60 + 20 + 40 + 60) / 4
    assert result.loc[1, "league_avg_stat_before"] == 45

    # 2023 reset
    assert result.loc[2, "league_avg_stat_before"] == 40


# ---------------------------------------------------------
# Test 6: Output column and row count validation
# ---------------------------------------------------------
def test_output_schema_and_length():
    """
    Ensure output column exists and row count is preserved.
    """

    df = pd.DataFrame({
        "season": [2023],
        "game_date": ["2023-09-01"],
        "home_metric": [7],
        "away_metric": [3],
    })

    result = add_league_avg_stat_before(
        df,
        stat_name="metric",
        home_stat_col="home_metric",
        away_stat_col="away_metric",
    )

    assert "league_avg_metric_before" in result.columns
    assert len(result) == len(df)

# ---------------------------------------------------------
# Test 1: Single game baseline
# ---------------------------------------------------------
def test_single_game_pf_pa():
    """
    For a single game, PF and PA should equal the game score itself.
    """

    df = pd.DataFrame({
        "game_date": pd.to_datetime(["2023-09-01"]),
        "season": [2023],
        "home_team": ["A"],
        "away_team": ["B"],
        "total_home_score": [24],
        "total_away_score": [17],
    })

    result = add_pf_pa_by_season(df)

    assert result.loc[0, "home_pf"] == 24
    assert result.loc[0, "home_pa"] == 17
    assert result.loc[0, "away_pf"] == 17
    assert result.loc[0, "away_pa"] == 24


# ---------------------------------------------------------
# Test 2: Multiple games same teams (season cumulative)
# ---------------------------------------------------------
def test_multiple_games_same_teams_cumulative():
    """
    Verify PF/PA are cumulative within the same season.
    """

    df = pd.DataFrame({
        "game_date": pd.to_datetime(["2023-09-01", "2023-09-08"]),
        "season": [2023, 2023],
        "home_team": ["A", "A"],
        "away_team": ["B", "B"],
        "total_home_score": [20, 30],
        "total_away_score": [10, 40],
    })

    result = add_pf_pa_by_season(df)

    # Game 1
    assert result.loc[0, "home_pf"] == 20
    assert result.loc[0, "home_pa"] == 10
    assert result.loc[0, "away_pf"] == 10
    assert result.loc[0, "away_pa"] == 20

    # Game 2 (cumulative)
    assert result.loc[1, "home_pf"] == 50
    assert result.loc[1, "home_pa"] == 50
    assert result.loc[1, "away_pf"] == 50
    assert result.loc[1, "away_pa"] == 50


# ---------------------------------------------------------
# Test 3: Home / Away role symmetry
# ---------------------------------------------------------
def test_home_away_symmetry_pf_pa():
    """
    Ensure PF/PA accumulation works when teams switch home/away roles.
    """

    df = pd.DataFrame({
        "game_date": pd.to_datetime(["2023-09-01", "2023-09-08"]),
        "season": [2023, 2023],
        "home_team": ["A", "B"],
        "away_team": ["B", "A"],
        "total_home_score": [28, 14],
        "total_away_score": [14, 21],
    })

    result = add_pf_pa_by_season(df)

    # Team A totals: PF = 28 + 21, PA = 14 + 14
    assert result.loc[1, "away_pf"] == 49
    assert result.loc[1, "away_pa"] == 28

    # Team B totals: PF = 14 + 14, PA = 28 + 21
    assert result.loc[1, "home_pf"] == 28
    assert result.loc[1, "home_pa"] == 49


# ---------------------------------------------------------
# Test 4: Season reset logic
# ---------------------------------------------------------
def test_pf_pa_resets_each_season():
    """
    Ensure PF/PA reset when season changes.
    """

    df = pd.DataFrame({
        "game_date": pd.to_datetime(["2022-09-01", "2023-09-01"]),
        "season": [2022, 2023],
        "home_team": ["A", "A"],
        "away_team": ["B", "B"],
        "total_home_score": [35, 21],
        "total_away_score": [14, 7],
    })

    result = add_pf_pa_by_season(df)

    assert result.loc[0, "home_pf"] == 35
    assert result.loc[1, "home_pf"] == 21  # reset


# ---------------------------------------------------------
# Test 5: Sorting correctness with same-day games
# ---------------------------------------------------------
def test_sorting_with_same_game_date():
    """
    Ensure df_row is used to preserve deterministic ordering.
    """

    df = pd.DataFrame({
        "game_date": pd.to_datetime(["2023-09-01", "2023-09-01"]),
        "season": [2023, 2023],
        "home_team": ["A", "A"],
        "away_team": ["B", "B"],
        "total_home_score": [10, 30],
        "total_away_score": [20, 40],
    })

    result = add_pf_pa_by_season(df)

    assert result.loc[0, "home_pf"] == 10
    assert result.loc[1, "home_pf"] == 40


# ---------------------------------------------------------
# Test 6: Season auto-creation from game_date
# ---------------------------------------------------------
def test_season_created_from_game_date_if_missing():
    """
    Verify season column is inferred from game_date if missing.
    """

    df = pd.DataFrame({
        "game_date": pd.to_datetime(["2023-09-01"]),
        "home_team": ["A"],
        "away_team": ["B"],
        "total_home_score": [17],
        "total_away_score": [14],
    })

    result = add_pf_pa_by_season(df)

    assert "season" in result.columns
    assert result.loc[0, "home_pf"] == 17


# ---------------------------------------------------------
# Test 7: Output schema & row count validation
# ---------------------------------------------------------
def test_output_columns_and_length():
    """
    Ensure output columns exist and no rows are dropped.
    """

    df = pd.DataFrame({
        "game_date": pd.to_datetime(["2023-09-01"]),
        "season": [2023],
        "home_team": ["X"],
        "away_team": ["Y"],
        "total_home_score": [13],
        "total_away_score": [10],
    })

    result = add_pf_pa_by_season(df)

    expected_cols = {
        "home_pf", "home_pa", "away_pf", "away_pa"
    }

    assert expected_cols.issubset(result.columns)
    assert len(result) == len(df)# sync 1774962714359977439
# sync 1774962714470600224
# sync 1774962760551650379
# sync 1774962762003221549
# sync 1774962857936993600
