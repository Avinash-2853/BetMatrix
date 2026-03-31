# type: ignore
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
import pytest
import pandas as pd
import numpy as np
from typing import Any, Dict
from unittest.mock import patch
from app.pipeline.inference import (
    get_inputs,
    get_last_5_stats,
    get_prev_stat
)

# ------------------ Sample DataFrame ------------------
def get_sample_df() -> pd.DataFrame:
    data: Dict[str, Any] = {
        "game_date": ["2025-09-20", "2025-09-21", "2025-09-22", "2025-09-23", "2025-09-24"],
        "home_team": ["TeamA", "TeamB", "TeamA", "TeamB", "TeamA"],
        "away_team": ["TeamB", "TeamA", "TeamB", "TeamA", "TeamB"],
        "total_home_score": [21, 17, 24, 20, 28],
        "total_away_score": [14, 20, 10, 22, 21],
        "total_home_epa": [5, 3, 6, 2, 7],
        "total_away_epa": [3, 4, 2, 5, 3],
        "total_home_rush_epa": [2, 1, 3, 2, 2],
        "total_away_rush_epa": [1, 2, 1, 3, 1],
        "total_home_pass_epa": [3, 2, 3, 0, 5],
        "total_away_pass_epa": [2, 2, 1, 2, 2],
        "home_first_down_rush": [5, 4, 6, 3, 5],
        "away_first_down_rush": [3, 5, 2, 4, 3],
        "home_first_down_pass": [3, 2, 4, 1, 3],
        "away_first_down_pass": [2, 3, 1, 2, 2],
        "home_third_down_converted": [2, 3, 2, 2, 3],
        "away_third_down_converted": [1, 2, 1, 2, 1],
        "home_fourth_down_converted": [1, 0, 1, 1, 0],
        "away_fourth_down_converted": [0, 1, 0, 0, 1],
        "home_interception": [0, 1, 0, 1, 0],
        "away_interception": [1, 0, 1, 0, 1],
        "home_fumble_lost": [0, 0, 1, 0, 0],
        "away_fumble_lost": [0, 1, 0, 1, 0],
        "home_fumble_forced": [1, 0, 1, 0, 1],
        "away_fumble_forced": [0, 1, 0, 1, 0],
        "home_rush_attempt": [20, 18, 22, 19, 21],
        "away_rush_attempt": [18, 21, 17, 20, 19],
        "home_pass_attempt": [30, 25, 32, 28, 31],
        "away_pass_attempt": [28, 30, 27, 29, 26],
        "home_pass_touchdown": [2, 1, 3, 1, 2],
        "away_pass_touchdown": [1, 2, 0, 2, 1],
        "home_qb_dropback": [35, 32, 36, 30, 34],
        "away_qb_dropback": [33, 34, 31, 35, 32],
        "home_rush_touchdown": [1, 0, 1, 0, 1],
        "away_rush_touchdown": [0, 1, 0, 1, 0],
        "home_tackled_for_loss": [2, 1, 3, 2, 2],
        "away_tackled_for_loss": [1, 2, 1, 3, 1],
        "home_qb_hit": [3, 2, 4, 1, 3],
        "away_qb_hit": [2, 3, 1, 3, 2],
        "home_punt_attempt": [2, 1, 2, 1, 2],
        "away_punt_attempt": [1, 2, 1, 2, 1],
        "home_kickoff_attempt": [1, 0, 1, 0, 1],
        "away_kickoff_attempt": [0, 1, 0, 1, 0],
        "home_kickoff_inside_twenty": [1, 0, 1, 0, 1],
        "away_kickoff_inside_twenty": [0, 1, 0, 1, 0],
        "home_penalty_yards": [10, 8, 12, 7, 9],
        "away_penalty_yards": [12, 9, 11, 8, 10],
        "home_rushing_yards": [120, 110, 130, 115, 125],
        "away_rushing_yards": [110, 120, 105, 125, 115],
        "home_passing_yards": [250, 230, 270, 240, 260],
        "away_passing_yards": [240, 250, 230, 245, 235],
        "home_receiving_yards": [200, 180, 210, 190, 205],
        "away_receiving_yards": [190, 200, 185, 195, 185],
        "home_yards_gained": [370, 340, 400, 355, 390],
        "away_yards_gained": [350, 370, 335, 370, 340],
        "home_sack": [2, 1, 3, 2, 2],
        "away_sack": [1, 2, 1, 2, 1],
        "home_return_yards_positive": [30, 25, 32, 28, 30],
        "away_return_yards_positive": [25, 28, 24, 30, 27],
        "home_return_yards_negative": [-5, -6, -4, -5, -5],
        "away_return_yards_negative": [-6, -5, -7, -4, -6],
        "home_yards_after_catch_positive": [50, 45, 52, 48, 50],
        "away_yards_after_catch_positive": [45, 48, 43, 47, 44],
        "home_yards_after_catch_negative": [-10, -12, -9, -11, -10],
        "away_yards_after_catch_negative": [-11, -10, -13, -9, -12],
        "home_team_glicko_rating": [1500, 1450, 1520, 1470, 1510],
        "away_team_glicko_rating": [1450, 1500, 1480, 1520, 1490],
        "home_team_rd": [30, 28, 32, 29, 31],
        "away_team_rd": [28, 30, 29, 32, 30],
        "home_team_vol": [0.06, 0.05, 0.07, 0.05, 0.06],
        "away_team_vol": [0.05, 0.06, 0.05, 0.07, 0.05],
        "home_pct": [0.6, 0.5, 0.7, 0.4, 0.6],
        "away_pct": [0.5, 0.6, 0.4, 0.7, 0.5],
        "home_pf": [300, 280, 320, 250, 310],
        "away_pf": [280, 300, 260, 310, 290],
        "home_pa": [250, 260, 240, 280, 250],
        "away_pa": [260, 250, 280, 240, 270],
        "home_team_avg_score": [25, 24, 28, 20, 26],
        "away_team_avg_score": [24, 25, 20, 28, 24],
        "home_net_rating": [5, 2, 8, -3, 6],
        "away_net_rating": [2, 5, -2, 7, 2],
        "home_defense": [10, 12, 9, 15, 11],
        "away_defense": [12, 10, 14, 8, 12],
        "home_offense": [20, 18, 22, 16, 21],
        "away_offense": [18, 20, 16, 22, 18],
        "home_team_peformance": [0.8, 0.7, 0.9, 0.6, 0.8],
        "away_team_peformance": [0.7, 0.8, 0.6, 0.9, 0.7],
        "home_avg_passing_yards": [240, 230, 250, 220, 245],
        "away_avg_passing_yards": [230, 240, 220, 250, 235],
        "home_avg_receiving_yards": [240, 230, 250, 220, 245],
        "away_avg_receiving_yards": [230, 240, 220, 250, 235],
        "home_avg_rushing_yards": [120, 110, 130, 100, 125],
        "away_avg_rushing_yards": [110, 120, 100, 130, 115],
        "home_avg_yards_gained": [360, 340, 380, 320, 370],
        "away_avg_yards_gained": [340, 360, 320, 380, 350],
    }
    df = pd.DataFrame(data)
    return df

# ------------------ Pytest Fixtures ------------------
@pytest.fixture
def full_sample_df() -> pd.DataFrame:
    return get_sample_df()


def test_get_head_to_head_win_ratio_edge(monkeypatch: Any, full_sample_df: pd.DataFrame) -> None:
    monkeypatch.setattr("pandas.read_csv", lambda *args, **kwargs: full_sample_df)
    # Ensure os.path.exists returns True for the check inside get_inputs
    with patch("os.path.exists", return_value=True):
        df = get_inputs("TeamA", "TeamB", "CoachA", "CoachB", "StadiumX", game_date="2025-09-25")
    
    assert "h2h_home_win_ratio" in df.columns
    assert "h2h_away_win_ratio" in df.columns
    # With full_sample_df, TeamA wins all encounters against TeamB
    # So h2h_home_win_ratio should be 1.0 (TeamA is home in args)
    assert np.isclose(df["h2h_home_win_ratio"].iloc[0], 1.0)
    assert np.isclose(df["h2h_away_win_ratio"].iloc[0], 0.0)

def test_get_prev_stat_home(full_sample_df: pd.DataFrame) -> None:
    # Test get_prev_stat (single most recent)
    val = get_prev_stat(full_sample_df, "TeamA", "total_home_score", "total_away_score")
    # Most recent game for TeamA is 2025-09-24 (Home), score 28
    assert val == 28.0

def test_get_prev_stat_away(full_sample_df: pd.DataFrame) -> None:
    # Test get_prev_stat for TeamB
    val = get_prev_stat(full_sample_df, "TeamB", "total_home_score", "total_away_score")
    # Most recent game for TeamB is 2025-09-24 (Away), score 21 (total_away_score)
    assert val == 21.0

def test_get_last_5_stat_home(full_sample_df: pd.DataFrame) -> None:
    # Test get_last_5_stats (average)
    val = get_last_5_stats(full_sample_df, "TeamA", "total_home_score", "total_away_score")
    # TeamA scores:
    # 9-24: 28
    # 9-23: 22
    # 9-22: 24
    # 9-21: 20
    # 9-20: 21
    # Avg: (28+22+24+20+21)/5 = 115/5 = 23.0
    assert val > 0
    assert np.isclose(val, 23.0)

def test_get_last_5_stat_away(full_sample_df: pd.DataFrame) -> None:
    val = get_last_5_stats(full_sample_df, "TeamB", "total_home_score", "total_away_score")
    # TeamB scores: 21, 20, 10, 17, 14 => Avg 16.4
    assert val > 0
    assert np.isclose(val, 16.4)

def test_get_glicko_rating_home_away(monkeypatch: Any, full_sample_df: pd.DataFrame) -> None:
    monkeypatch.setattr("pandas.read_csv", lambda *args, **kwargs: full_sample_df)
    with patch("os.path.exists", return_value=True):
        df = get_inputs("TeamA", "TeamB", "CoachA", "CoachB", "StadiumX", game_date="2025-09-25")
        
    assert "prev_home_team_glicko_rating" in df.columns
    # TeamA (Home) most recent glicko: 2025-09-24 (H) -> 1510
    assert df["prev_home_team_glicko_rating"].iloc[0] == 1510.0
    
    assert "prev_away_team_glicko_rating" in df.columns
    # TeamB (Away) most recent glicko: 2025-09-24 (A) -> 1490
    assert df["prev_away_team_glicko_rating"].iloc[0] == 1490.0

def test_get_inputs(monkeypatch: Any, full_sample_df: pd.DataFrame) -> None:
    monkeypatch.setattr("pandas.read_csv", lambda *args, **kwargs: full_sample_df)
    df = get_inputs("TeamA", "TeamB", "CoachA", "CoachB", "StadiumX", game_date="2025-09-25")
    assert df["home_team"].iloc[0] == "TeamA"
    assert df["away_team"].iloc[0] == "TeamB"
    assert df["home_coach"].iloc[0] == "CoachA"
    assert df["away_coach"].iloc[0] == "CoachB"
    assert "last_5_total_home_score" in df.columns
    assert "prev_total_home_score" in df.columns
    assert "prev_home_team_glicko_rating" in df.columns


@patch("app.pipeline.inference.os.path.exists", return_value=False)
def test_get_inputs_missing_csv(_mock_exists: Any) -> None:
    with pytest.raises(FileNotFoundError) as excinfo:
        get_inputs("TeamA", "TeamB", "CoachA", "CoachB", "StadiumX", game_date="2025-09-25")
    assert "Input data not found" in str(excinfo.value)


def test_fewer_than_5_games() -> None:
    df = pd.DataFrame({
        "game_date": ["2025-09-20", "2025-09-19"],
        "home_team": ["TeamA", "TeamB"],
        "away_team": ["TeamB", "TeamA"],
        "total_home_score": [20, 10],
        "total_away_score": [10, 15]
    })
    
    # TeamA games:
    # 9-20 (H): 20
    # 9-19 (A): 15
    # Avg: 17.5
    
    val = get_last_5_stats(df, "TeamA", "total_home_score", "total_away_score")
    assert np.isclose(val, 17.5)


def test_only_away_games(full_sample_df: pd.DataFrame) -> None:
    # TeamB has mixed home/away games, but let's assume we want to test that away games are counted
    val = get_last_5_stats(full_sample_df, "TeamB", "total_home_score", "total_away_score")
    assert val > 0

def test_get_prev_stat_home_single_game() -> None:
    df = pd.DataFrame({
        "game_date": ["2025-09-20"],
        "home_team": ["TeamA"],
        "away_team": ["TeamB"],
        "total_home_score": [25],
        "total_away_score": [10]
    })
    # TeamA (Home): 25
    val = get_prev_stat(df, "TeamA", "total_home_score", "total_away_score")
    assert val == 25.0


def test_get_prev_stat_away_home_feature_branch() -> None:
    df = pd.DataFrame({
        "game_date": ["2025-09-20"],
        "home_team": ["TeamA"],
        "away_team": ["TeamB"],
        "total_home_score": [30],
        "total_away_score": [14]
    })
    # TeamA (Home): 30
    val = get_prev_stat(df, "TeamA", "total_home_score", "total_away_score")
    assert val == 30.0


def test_get_prev_stat_away_away_feature_branch() -> None:
    df = pd.DataFrame({
        "game_date": ["2025-09-20"],
        "home_team": ["TeamA"],
        "away_team": ["TeamB"],
        "total_home_score": [30],
        "total_away_score": [17]
    })
    # TeamB (Away): 17
    val = get_prev_stat(df, "TeamB", "total_home_score", "total_away_score")
    assert val == 17.0


def test_get_inputs_no_leakage(monkeypatch: Any, full_sample_df: pd.DataFrame) -> None:
    """
    Verify that get_inputs does not peek at the target game's data.
    Target game: 2025-09-24 (TeamA vs TeamB, score 28-21).
    This game exists in the CSV (full_sample_df).
    We want to ensure that stats from this game are NOT included in the inputs.
    """
    monkeypatch.setattr("pandas.read_csv", lambda *args, **kwargs: full_sample_df)
    
    # Request inputs for the game on 2025-09-24
    df = get_inputs("TeamA", "TeamB", "CoachA", "CoachB", "StadiumX", game_date="2025-09-24")
    
    # Check 'prior' score avg for TeamA (Home Team)
    # History for TeamA before 2025-09-24:
    # 2025-09-20: 21 points
    # 2025-09-21: 20 points
    # 2025-09-22: 24 points
    # 2025-09-23: 22 points
    # Average = (21 + 20 + 24 + 22) / 4 = 21.75
    
    # If it leaked (included 2025-09-24), it would be (21+20+24+22+28)/5 = 23.0
    
    # In new code, last_5_<feature> is the average. prev_<feature> is the single recent stat.
    assert "last_5_total_home_score" in df.columns
    # We expect 21.75
    
    actual_val = df["last_5_total_home_score"].iloc[0]
    
    # Allow small float tolerance
    assert np.isclose(actual_val, 21.75), f"Expected 21.75 (no leakage), got {actual_val}. Leakage might be happening!"
# sync 1774962714535685028
# sync 1774962761904356028
# sync 1774962763817330454
# sync 1774962858236631888
# sync 1774962859011183234
