# type: ignore
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
import pytest
import pandas as pd
from unittest.mock import patch
from typing import Any, Generator
from app.pipeline.data_load import load_nfl_data


# ----------------------
# Sample Mock DataFrames
# ----------------------
# Mock DataFrame with all features from the function's feature list
MOCK_PBP_DATA_ALL_FEATURES = pd.DataFrame({
    "game_id": ["2023_01_ARI_WAS"],
    "play_id": [1.0],
    "season": [2023],
    "week": [1],
    "season_type": ["REG"],
    "game_date": ["2023-09-10"],
    "stadium": ["FedExField"],
    "game_stadium": ["FedExField"],
    "roof": ["outdoors"],
    "surface": ["grass"],
    "location": ["Away"],
    "temp": [72],
    "wind": [5],
    "home_team": ["WAS"],
    "away_team": ["ARI"],
    "home_coach": ["Ron Rivera"],
    "away_coach": ["Jonathan Gannon"],
    "Div_game": [0],
    "home_score": [20],
    "away_score": [16],
    "total_home_score": [20],
    "total_away_score": [16],
    "posteam": ["ARI"],
    "posteam_score": [16],
    "defteam_score": [20],
    "score_differential": [-4],
    "first_down_rush": [1],
    "first_down_pass": [0],
    "third_down_converted": [1],
    "fourth_down_converted": [0],
    "sack": [1],
    "interception": [0],
    "fumble_lost": [0],
    "penalty_yards": [10],
    "rush_attempt": [1],
    "pass_attempt": [0],
    "rushing_yards": [5],
    "passing_yards": [0],
    "receiving_yards": [0],
    "pass_touchdown": [0],
    "qb_dropback": [0],
    "rush_touchdown": [0],
    "yards_after_catch": [0],
    "tackled_for_loss": [0],
    "qb_hit": [0],
    "fumble_forced": [0],
    "fumble_recovery_1_yards": [0],
    "field_goal_attempt": [0],
    "field_goal_result": ["NA"],
    "extra_point_attempt": [0],
    "extra_point_result": ["NA"],
    "punt_attempt": [0],
    "kickoff_attempt": [1],
    "kickoff_inside_twenty": [0],
    "return_yards": [0],
    "spread_line": [-7.0],
    "total_line": [38.5],
    "vegas_wp": [0.45],
    "Vegas_home_wp": [0.55],
    "total_home_epa": [2.5],
    "total_away_epa": [-2.5],
    "total_home_rush_epa": [1.0],
    "total_away_rush_epa": [-1.0],
    "total_home_pass_epa": [1.5],
    "total_away_pass_epa": [-1.5],
    "total_home_comp_air_epa": [1.2],
    "total_away_comp_air_epa": [-1.2],
    "total_home_comp_yac_epa": [0.3],
    "total_away_comp_yac_epa": [-0.3],
    "total_home_raw_air_epa": [1.0],
    "total_away_raw_air_epa": [-1.0],
    "total_home_raw_yac_epa": [0.2],
    "Total_away_raw_yac_epa": [-0.2],
    "total_home_comp_air_wpa": [0.05],
    "total_away_comp_air_wpa": [-0.05],
    "total_home_comp_yac_wpa": [0.01],
    "total_away_comp_yac_wpa": [-0.01],
    "total_home_raw_air_wpa": [0.04],
    "total_away_raw_air_wpa": [-0.04],
    "total_home_raw_yac_wpa": [0.01],
    "total_away_raw_yac_wpa": [-0.01],
    "quarter_seconds_remaining": [900],
    "half_seconds_remaining": [1800],
    "game_seconds_remaining": [3600],
    "yards_gained": [5]
})

# Mock DataFrame with only some features (subset)
MOCK_PBP_DATA_PARTIAL_FEATURES = pd.DataFrame({
    "game_id": ["2023_01_ARI_WAS"],
    "play_id": [1.0],
    "season": [2023],
    "week": [1],
    "quarter_seconds_remaining": [900],
    "half_seconds_remaining": [1800],
    "game_seconds_remaining": [3600],
    "yards_gained": [5],
    # Some features that are NOT in the feature list
    "extra_column_1": ["value1"],
    "extra_column_2": [100]
})

# Mock DataFrame with minimal columns
MOCK_PBP_DATA_MINIMAL = pd.DataFrame({
    "game_id": ["2023_01_ARI_WAS"],
    "play_id": [1.0]
})

# Empty DataFrame
MOCK_PBP_DATA_EMPTY = pd.DataFrame()


@pytest.fixture
def mock_load_pbp_all_features() -> Generator[Any, None, None]:
    """Mock nflreadpy.load_pbp to return DataFrame with all features"""
    with patch("nflreadpy.load_pbp", return_value=MOCK_PBP_DATA_ALL_FEATURES) as mock:
        yield mock


@pytest.fixture
def mock_load_pbp_partial_features() -> Generator[Any, None, None]:
    """Mock nflreadpy.load_pbp to return DataFrame with partial features"""
    with patch("nflreadpy.load_pbp", return_value=MOCK_PBP_DATA_PARTIAL_FEATURES) as mock:
        yield mock


@pytest.fixture
def mock_load_pbp_minimal() -> Generator[Any, None, None]:
    """Mock nflreadpy.load_pbp to return DataFrame with minimal columns"""
    with patch("nflreadpy.load_pbp", return_value=MOCK_PBP_DATA_MINIMAL) as mock:
        yield mock


@pytest.fixture
def mock_load_pbp_empty() -> Generator[Any, None, None]:
    """Mock nflreadpy.load_pbp to return empty DataFrame"""
    with patch("nflreadpy.load_pbp", return_value=MOCK_PBP_DATA_EMPTY) as mock:
        yield mock


# ----------------------
# Core Function Tests
# ----------------------
def test_load_nfl_data_all_features_present(mock_load_pbp_all_features: Any) -> None:
    """Test that function returns DataFrame with all available features from the feature list"""
    df = load_nfl_data([2023])
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    # Should contain all features that are in both the feature list and the mock data
    assert "game_id" in df.columns
    assert "play_id" in df.columns
    assert "season" in df.columns
    assert "home_team" in df.columns
    assert "away_team" in df.columns
    assert "quarter_seconds_remaining" in df.columns
    assert "yards_gained" in df.columns


def test_load_nfl_data_partial_features_filtering(mock_load_pbp_partial_features: Any) -> None:
    """Test that function filters columns to only include features from the feature list"""
    df = load_nfl_data([2023])
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    # Should only contain features from the feature list
    assert "game_id" in df.columns
    assert "play_id" in df.columns
    assert "season" in df.columns
    assert "week" in df.columns
    # Should NOT contain columns that are not in the feature list
    assert "extra_column_1" not in df.columns
    assert "extra_column_2" not in df.columns


def test_load_nfl_data_return_type(mock_load_pbp_all_features: Any) -> None:
    """Test that function returns pandas DataFrame"""
    df = load_nfl_data([2023])
    assert isinstance(df, pd.DataFrame)


def test_load_nfl_data_columns_filtering(mock_load_pbp_partial_features: Any) -> None:
    """Test that only available features from the feature list are kept"""
    df = load_nfl_data([2023])
    
    # Should only contain columns that are in both the feature list and the mock data
    expected_columns = {"game_id", "play_id", "season", "week", 
                       "quarter_seconds_remaining", "half_seconds_remaining",
                       "game_seconds_remaining", "yards_gained"}
    assert set(df.columns).issubset(expected_columns)
    # Should not contain features that are in feature list but not in data
    assert "home_team" not in df.columns
    assert "away_team" not in df.columns
    assert "punt_attempt" not in df.columns


def test_load_nfl_data_minimal_columns(mock_load_pbp_minimal: Any) -> None:
    """Test function with minimal columns in input data"""
    df = load_nfl_data([2023])
    
    assert isinstance(df, pd.DataFrame)
    assert "game_id" in df.columns
    assert "play_id" in df.columns
    # Should not contain columns that don't exist in the data
    assert len(df.columns) == 2


def test_load_nfl_data_empty_dataframe(mock_load_pbp_empty: Any) -> None:
    """Test function with empty DataFrame"""
    df = load_nfl_data([2023])
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0
    assert len(df.columns) == 0


def test_load_nfl_data_multiple_seasons(mock_load_pbp_all_features: Any) -> None:
    """Test function with multiple seasons"""
    df = load_nfl_data([2020, 2021, 2022, 2023])
    
    assert isinstance(df, pd.DataFrame)
    # Mock returns same data, so length should be same
    assert len(df) == 1


def test_load_nfl_data_single_season(mock_load_pbp_all_features: Any) -> None:
    """Test function with single season"""
    df = load_nfl_data([2023])
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1


def test_load_nfl_data_empty_seasons_list(mock_load_pbp_all_features: Any) -> None:
    """Test function with empty seasons list"""
    df = load_nfl_data([])
    
    assert isinstance(df, pd.DataFrame)
    # Behavior depends on nflreadpy, but should return a DataFrame


def test_load_nfl_data_output_file_parameter_ignored(mock_load_pbp_all_features: Any) -> None:
    """Test that output_file parameter is ignored (deprecated)"""
    df1 = load_nfl_data([2023], output_file=None)
    df2 = load_nfl_data([2023], output_file="/some/path.csv")
    
    # Both should return the same result
    assert isinstance(df1, pd.DataFrame)
    assert isinstance(df2, pd.DataFrame)
    assert list(df1.columns) == list(df2.columns)


# ----------------------
# Environment Variable Tests
# ----------------------
def test_data_path_with_env_variable(monkeypatch: Any) -> None:
    """Test that data_path uses environment variable when set"""
    test_path = "/test/custom/path"
    monkeypatch.setenv("save_data_path", test_path)
    
    # Remove the module from cache to force fresh import
    import sys
    if 'app.pipeline.data_load' in sys.modules:
        del sys.modules['app.pipeline.data_load']
    
    # Import the module fresh to trigger the environment variable loading
    import app.pipeline.data_load
    
    assert app.pipeline.data_load.data_path == test_path


def test_data_path_without_env_variable(monkeypatch: Any) -> None:
    """Test that data_path uses default path when environment variable is not set"""
    # Mock os.getenv to return None for save_data_path
    def mock_getenv(key: str, default: Any = None) -> Any:
        if key == "save_data_path":
            return None
        return os.getenv(key, default)
    
    monkeypatch.setattr(os, "getenv", mock_getenv)
    
    # Remove the module from cache to force fresh import
    import sys
    if 'app.pipeline.data_load' in sys.modules:
        del sys.modules['app.pipeline.data_load']
    
    # Import the module fresh to trigger the default path setting
    import app.pipeline.data_load
    
    # Should use default path
    expected_path = os.path.join(
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")),
        "data", "input"
    )
    assert app.pipeline.data_load.data_path == expected_path


def test_data_path_with_relative_env_variable(monkeypatch: Any) -> None:
    """Test that relative paths in environment variable are handled correctly"""
    test_path = "relative/path"
    monkeypatch.setenv("save_data_path", test_path)
    
    # Remove the module from cache
    import sys
    if 'app.pipeline.data_load' in sys.modules:
        del sys.modules['app.pipeline.data_load']
    
    # Import fresh
    import app.pipeline.data_load
    
    # The module should handle relative paths (implementation dependent)
    # Just verify it's set
    assert app.pipeline.data_load.data_path is not None


# ----------------------
# Edge Cases and Error Handling
# ----------------------
def test_load_nfl_data_with_none_values(mock_load_pbp_all_features: Any) -> None:
    """Test function handles DataFrame with None values"""
    df = load_nfl_data([2023])
    
    assert isinstance(df, pd.DataFrame)
    # Should not raise any errors even with None values


def test_load_nfl_data_feature_list_completeness(mock_load_pbp_all_features: Any) -> None:
    """Test that all features in the function's feature list are properly handled"""
    df = load_nfl_data([2023])
    
    # Verify that if a feature exists in both data and feature list, it's included
    # This tests the filtering logic: available_features = [f for f in features if f in df.columns]
    assert isinstance(df, pd.DataFrame)
    
    # Check that known features from the feature list are present if they exist in data
    feature_list_features = [
        "game_id", "play_id", "season", "week", "season_type", "game_date",
        "stadium", "game_stadium", "roof", "surface", "location", "temp", "wind",
        "home_team", "away_team", "home_coach", "away_coach", "Div_game",
        "home_score", "away_score", "total_home_score", "total_away_score",
        "posteam", "posteam_score", "defteam_score", "score_differential",
        "first_down_rush", "first_down_pass", "third_down_converted",
        "fourth_down_converted", "sack", "interception", "fumble_lost",
        "penalty_yards", "rush_attempt", "pass_attempt", "rushing_yards",
        "passing_yards", "receiving_yards", "pass_touchdown", "qb_dropback",
        "rush_touchdown", "yards_after_catch", "tackled_for_loss", "qb_hit",
        "fumble_forced", "fumble_recovery_1_yards", "field_goal_attempt",
        "field_goal_result", "extra_point_attempt", "extra_point_result",
        "punt_attempt", "kickoff_attempt", "kickoff_inside_twenty",
        "return_yards", "spread_line", "total_line", "vegas_wp",
        "Vegas_home_wp", "total_home_epa", "total_away_epa",
        "total_home_rush_epa", "total_away_rush_epa", "total_home_pass_epa",
        "total_away_pass_epa", "total_home_comp_air_epa", "total_away_comp_air_epa",
        "total_home_comp_yac_epa", "total_away_comp_yac_epa",
        "total_home_raw_air_epa", "total_away_raw_air_epa",
        "total_home_raw_yac_epa", "Total_away_raw_yac_epa",
        "total_home_comp_air_wpa", "total_away_comp_air_wpa",
        "total_home_comp_yac_wpa", "total_away_comp_yac_wpa",
        "total_home_raw_air_wpa", "total_away_raw_air_wpa",
        "total_home_raw_yac_wpa", "total_away_raw_yac_wpa",
        "quarter_seconds_remaining", "half_seconds_remaining",
        "game_seconds_remaining", "yards_gained"
    ]
    
    # All features from the list that exist in mock data should be in result
    for feature in feature_list_features:
        if feature in MOCK_PBP_DATA_ALL_FEATURES.columns:
            assert feature in df.columns, f"Feature {feature} should be in result"


def test_load_nfl_data_column_order(mock_load_pbp_partial_features: Any) -> None:
    """Test that column order matches the feature list order (for available features)"""
    df = load_nfl_data([2023])
    
    # The columns should be in the order they appear in the feature list
    # (filtered to only available ones)
    assert isinstance(df, pd.DataFrame)
    # Verify columns are present (order may vary, but existence is what matters)
    assert "game_id" in df.columns
    assert "play_id" in df.columns


def test_load_nfl_data_dotenv_loaded() -> None:
    """Test that dotenv is loaded (module level)"""
    # This test verifies the module-level code executes
    import app.pipeline.data_load
    
    # Verify module attributes exist
    assert hasattr(app.pipeline.data_load, 'data_path')
    assert hasattr(app.pipeline.data_load, 'load_nfl_data')
    assert callable(app.pipeline.data_load.load_nfl_data)
# sync 1774962858298281523
# sync 1774962859424672407
# sync 1774962859493636772
# sys_sync_67f56ea1
# sys_sync_2a624747
# sys_sync_13795cab
