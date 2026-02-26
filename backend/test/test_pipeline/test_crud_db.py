# type: ignore
import pytest
import requests
from unittest.mock import patch, MagicMock, call
from typing import Dict, Any
import sys
import os

# Correctly append the project root ('backend') to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

# Assume the functions are now in a file named app/pipeline/get_data.py
from app.pipeline.get_data import (
    get_coach_name,
    get_team_logo,
    map_team,
    get_team_details,
    get_match_scores,
)

# --------------------
# Tests for get_coach_name
# --------------------

@patch('requests.get')
def test_get_coach_name_success(mock_get: MagicMock) -> None:
    """Tests successful retrieval of a coach's name."""
    # Clear cache before running the test to ensure a clean state
    get_coach_name.cache_clear()

    # Mock response for the initial coaches URL
    mock_resp_initial = MagicMock()
    mock_resp_initial.json.return_value = {
        "items": [{"$ref": "http://example.com/coach_details"}]
    }

    # Mock response for the coach details URL
    mock_resp_details = MagicMock()
    mock_resp_details.json.return_value = {"firstName": "John", "lastName": "Harbaugh"}

    # Configure the mock to return different values for different calls
    mock_get.side_effect = [mock_resp_initial, mock_resp_details]

    first_name, last_name = get_coach_name("1", 2023)
    _ = (first_name, last_name)  # Handle unused return values

    assert first_name == "John"
    assert last_name == "Harbaugh"
    assert mock_get.call_count == 2
    mock_get.assert_any_call("https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2023/teams/1/coaches", timeout=10)
    mock_get.assert_any_call("http://example.com/coach_details", timeout=10)

@patch('requests.get')
def test_get_coach_name_lru_cache(mock_get: MagicMock) -> None:
    """Tests that lru_cache prevents redundant API calls."""
    get_coach_name.cache_clear()

    mock_resp_initial = MagicMock()
    mock_resp_initial.json.return_value = {"items": [{"$ref": "http://example.com/coach"}]}
    mock_resp_details = MagicMock()
    mock_resp_details.json.return_value = {"firstName": "Andy", "lastName": "Reid"}
    mock_get.side_effect = [mock_resp_initial, mock_resp_details, mock_resp_initial, mock_resp_details]


    # First call - should trigger API calls
    _ = get_coach_name("2", 2022)
    assert mock_get.call_count == 2

    # Second call with same arguments - should use cache
    _ = get_coach_name("2", 2022)
    assert mock_get.call_count == 2 # No new calls

@patch('requests.get')
def test_get_coach_name_api_error(mock_get: MagicMock) -> None:
    """Tests handling of a request exception."""
    get_coach_name.cache_clear()
    
    # Mock a successful response for the first call, and an exception for the second.
    # This ensures the exception is raised inside the function's try-except block.
    mock_resp_initial = MagicMock()
    mock_resp_initial.json.return_value = {
        "items": [{"$ref": "http://example.com/coach_details"}]
    }
    mock_get.side_effect = [mock_resp_initial, requests.RequestException]

    first_name, last_name = get_coach_name("3", 2021)
    _ = (first_name, last_name)  # Handle unused return values
    assert first_name is None
    assert last_name is None

@patch('requests.get')
def test_get_coach_name_malformed_response(mock_get: MagicMock) -> None:
    """Tests handling of responses with missing keys."""
    get_coach_name.cache_clear()
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"items": []} # Empty list, will cause IndexError
    mock_get.return_value = mock_resp
    first_name, last_name = get_coach_name("4", 2020)
    _ = (first_name, last_name)  # Handle unused return values
    assert first_name is None
    assert last_name is None

# --------------------
# Tests for get_team_logo
# --------------------

@patch('requests.get')
def test_get_team_logo_success(mock_get: MagicMock) -> None:
    """Tests successful retrieval of a team logo."""
    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        "logos": [{"href": "http://example.com/logo.png"}]
    }
    mock_get.return_value = mock_resp

    logo_url = get_team_logo("1", 2023)
    assert logo_url == "http://example.com/logo.png"
    mock_get.assert_called_once_with("https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2023/teams/1?lang=en&region=us", timeout=10)

@patch('requests.get')
def test_get_team_logo_api_error(mock_get: MagicMock) -> None:
    """Tests handling of a request exception."""
    mock_get.side_effect = requests.RequestException
    logo_url = get_team_logo("2", 2022)
    assert logo_url is None

@patch('requests.get')
def test_get_team_logo_no_logos(mock_get: MagicMock) -> None:
    """Tests handling when the 'logos' key is missing or empty."""
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"logos": []} # Empty list
    mock_get.return_value = mock_resp
    assert get_team_logo("3", 2021) is None

    mock_resp.json.return_value = {} # Missing 'logos' key
    mock_get.return_value = mock_resp
    assert get_team_logo("4", 2020) is None

# --------------------
# Tests for map_team
# --------------------

def test_map_team() -> None:
    """Tests the team name mapping logic."""
    assert map_team("WSH") == "WAS"
    assert map_team("LAR") == "LA"
    assert map_team("KC") == "KC"
    assert map_team("SF") == "SF"

# --------------------
# Tests for get_team_details
# --------------------

@patch('app.pipeline.get_data.get_coach_name')
@patch('app.pipeline.get_data.get_team_logo')
@patch('requests.get')
def test_get_team_details_success(mock_requests_get: MagicMock, mock_get_logo: MagicMock, mock_get_coach: MagicMock) -> None:
    """Tests successful fetching and processing of team details."""
    mock_schedule_data: Dict[str, Any] = {
        "content": {
            "schedule": {
                "20231012": {
                    "games": [{
                        "competitions": [{
                            "id": "401547374",
                            "venue": {"fullName": "Arrowhead Stadium"},
                            "competitors": [
                                {"homeAway": "home", "team": {"id": "12", "abbreviation": "KC"}},
                                {"homeAway": "away", "team": {"id": "7", "abbreviation": "DEN"}}
                            ]
                        }]
                    }]
                }
            }
        }
    }
    mock_resp = MagicMock()
    mock_resp.json.return_value = mock_schedule_data
    mock_requests_get.return_value = mock_resp

    mock_get_logo.side_effect = ["http://logo.com/kc.png", "http://logo.com/den.png"]
    mock_get_coach.side_effect = [("Andy", "Reid"), ("Sean", "Payton")]

    games = get_team_details(2023, 1)

    assert len(games) == 1
    game = games[0]
    assert game["game_id"] == "401547374"
    assert game["home_team"] == "KC"
    assert game["away_team"] == "DEN"
    assert game["stadium"] == "Arrowhead Stadium"
    assert game["home_coach"] == "Andy Reid"
    assert game["away_coach"] == "Sean Payton"
    assert game["home_team_logo_url"] == "http://logo.com/kc.png"
    assert game["away_team_logo_url"] == "http://logo.com/den.png"

@patch('requests.get')
def test_get_team_details_missing_teams(mock_requests_get: MagicMock) -> None:
    """Tests that games with incomplete competitor data are skipped."""
    mock_schedule_data = {
        "content": {"schedule": {"date": {"games": [
            {"competitions": [{"competitors": [{"homeAway": "home"}]}]} # Missing away
        ]}}}
    }
    mock_resp = MagicMock()
    mock_resp.json.return_value = mock_schedule_data
    mock_requests_get.return_value = mock_resp

    games = get_team_details(2023, 1)
    assert len(games) == 0

# --------------------
# Tests for get_match_scores
# --------------------

@patch('requests.get')
def test_get_match_scores_success(mock_get: MagicMock) -> None:
    """Tests successful retrieval of match scores."""
    mock_event_data: Dict[str, Any] = {
        "competitions": [{"competitors": [
            {"homeAway": "home", "score": {"$ref": "http://score.com/home"}},
            {"homeAway": "away", "score": {"$ref": "http://score.com/away"}}
        ]}]
    }
    mock_home_score = {"value": 27}
    mock_away_score = {"value": 17}

    mock_get.side_effect = [
        MagicMock(json=lambda: mock_event_data),
        MagicMock(json=lambda: mock_home_score),
        MagicMock(json=lambda: mock_away_score)
    ]

    scores = get_match_scores("event_id_123")
    assert scores == {"home_score": 27, "away_score": 17}
    expected_calls = [
        call("https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/event_id_123", timeout=10),
        call("http://score.com/home", timeout=10),
        call("http://score.com/away", timeout=10)
    ]
    mock_get.assert_has_calls(expected_calls, any_order=True)

@patch('requests.get')
def test_get_match_scores_api_error(mock_get: MagicMock) -> None:
    """Tests handling of a request exception."""
    mock_get.side_effect = requests.exceptions.RequestException
    assert get_match_scores("event_id_123") is None

@patch('requests.get')
def test_get_match_scores_parsing_error(mock_get: MagicMock) -> None:
    """Tests handling of various parsing errors (KeyError, IndexError)."""
    # Test KeyError
    mock_get.return_value = MagicMock(json=lambda: {})
    assert get_match_scores("event_id_123") is None

    # Test IndexError
    mock_get.return_value = MagicMock(json=lambda: {"competitions": []})
    assert get_match_scores("event_id_123") is None

@patch('requests.get')
def test_get_match_scores_incomplete_data(mock_get: MagicMock) -> None:
    """Tests handling of incomplete competitor or score data."""
    # Only one competitor
    mock_event_data = {"competitions": [{"competitors": [{"homeAway": "home"}]}]}
    mock_get.return_value = MagicMock(json=lambda: mock_event_data)
    assert get_match_scores("event_id_123") is None

    # Missing one score
    mock_event_data_missing_score: Dict[str, Any] = {
        "competitions": [{"competitors": [
            {"homeAway": "home", "score": {"$ref": "http://score.com/home"}},
            {"homeAway": "away"} # Missing score key
        ]}]
    }
    mock_home_score = {"value": 27}
    mock_get.side_effect = [
        MagicMock(json=lambda: mock_event_data_missing_score),
        MagicMock(json=lambda: mock_home_score),
    ]
    assert get_match_scores("event_id_123") is None

@patch('requests.get')
def test_get_match_scores_unexpected_error(mock_get: MagicMock) -> None:
    """Tests handling of a generic, unexpected exception."""
    mock_get.side_effect = Exception("A wild error appears!")
    assert get_match_scores("event_id_123") is None

# sync 1774962759750787003
# sync 1774962762085164233
# sync 1774962859213549085
# sys_sync_67c57848
# sys_sync_3e342228
# sys_sync_1ab05ed7
# sys_sync_68446f3e
# sys_sync_36702234
# sys_sync_29582e12
