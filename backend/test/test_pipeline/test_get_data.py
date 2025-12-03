# type: ignore
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
    get_game_date,
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

@patch('requests.get')
def test_get_match_scores_score_value_none(mock_get: MagicMock) -> None:
    """Tests handling when score value is None (should not default to 0)."""
    mock_event_data: Dict[str, Any] = {
        "competitions": [{"competitors": [
            {"homeAway": "home", "score": {"$ref": "http://score.com/home"}},
            {"homeAway": "away", "score": {"$ref": "http://score.com/away"}}
        ]}]
    }
    # One score returns None, the other has a value
    mock_home_score = {"value": None}
    mock_away_score = {"value": 17}
    
    mock_get.side_effect = [
        MagicMock(json=lambda: mock_event_data),
        MagicMock(json=lambda: mock_home_score),
        MagicMock(json=lambda: mock_away_score)
    ]
    
    # Should return None because both scores are required
    assert get_match_scores("event_id_123") is None

@patch('requests.get')
def test_get_match_scores_score_value_error(mock_get: MagicMock) -> None:
    """Tests handling when score value cannot be converted to int."""
    mock_event_data: Dict[str, Any] = {
        "competitions": [{"competitors": [
            {"homeAway": "home", "score": {"$ref": "http://score.com/home"}},
            {"homeAway": "away", "score": {"$ref": "http://score.com/away"}}
        ]}]
    }
    # Score value is invalid (cannot be converted to int)
    mock_home_score = {"value": "invalid"}
    mock_away_score = {"value": 17}
    
    mock_get.side_effect = [
        MagicMock(json=lambda: mock_event_data),
        MagicMock(json=lambda: mock_home_score),
        MagicMock(json=lambda: mock_away_score)
    ]
    
    # Should return None because ValueError will be raised
    assert get_match_scores("event_id_123") is None

@patch('requests.get')
def test_get_match_scores_fetch_score_api_error(mock_get: MagicMock) -> None:
    """Tests handling when fetching individual score fails."""
    mock_event_data: Dict[str, Any] = {
        "competitions": [{"competitors": [
            {"homeAway": "home", "score": {"$ref": "http://score.com/home"}},
            {"homeAway": "away", "score": {"$ref": "http://score.com/away"}}
        ]}]
    }
    
    mock_get.side_effect = [
        MagicMock(json=lambda: mock_event_data),
        requests.exceptions.RequestException("API error"),
        MagicMock(json=lambda: {"value": 17})
    ]
    
    # Should return None because one score fetch failed
    assert get_match_scores("event_id_123") is None

@patch('requests.get')
def test_get_match_scores_missing_score_ref(mock_get: MagicMock) -> None:
    """Tests handling when score $ref is missing."""
    mock_event_data: Dict[str, Any] = {
        "competitions": [{"competitors": [
            {"homeAway": "home", "score": {}},  # Missing $ref
            {"homeAway": "away", "score": {"$ref": "http://score.com/away"}}
        ]}]
    }
    mock_away_score = {"value": 17}
    
    mock_get.side_effect = [
        MagicMock(json=lambda: mock_event_data),
        MagicMock(json=lambda: mock_away_score)
    ]
    
    # Should return None because both scores are required
    assert get_match_scores("event_id_123") is None

@patch('requests.get')
def test_get_match_scores_missing_homeaway(mock_get: MagicMock) -> None:
    """Tests handling when homeAway is missing."""
    mock_event_data: Dict[str, Any] = {
        "competitions": [{"competitors": [
            {"score": {"$ref": "http://score.com/home"}},  # Missing homeAway
            {"homeAway": "away", "score": {"$ref": "http://score.com/away"}}
        ]}]
    }
    mock_away_score = {"value": 17}
    
    mock_get.side_effect = [
        MagicMock(json=lambda: mock_event_data),
        MagicMock(json=lambda: mock_away_score)
    ]
    
    # Should return None because both scores are required
    assert get_match_scores("event_id_123") is None

@patch('requests.get')
def test_get_match_scores_only_one_score_returned(mock_get: MagicMock) -> None:
    """Tests handling when only one score is successfully fetched."""
    mock_event_data: Dict[str, Any] = {
        "competitions": [{"competitors": [
            {"homeAway": "home", "score": {"$ref": "http://score.com/home"}},
            {"homeAway": "away", "score": {"$ref": "http://score.com/away"}}
        ]}]
    }
    # Only home score is available
    mock_home_score = {"value": 27}
    
    mock_get.side_effect = [
        MagicMock(json=lambda: mock_event_data),
        MagicMock(json=lambda: mock_home_score),
        requests.exceptions.RequestException("API error")
    ]
    
    # Should return None because both scores are required
    assert get_match_scores("event_id_123") is None

@patch('app.pipeline.get_data.get_coach_name')
@patch('app.pipeline.get_data.get_team_logo')
@patch('requests.get')
def test_get_team_details_missing_venue(mock_requests_get: MagicMock, mock_get_logo: MagicMock, mock_get_coach: MagicMock) -> None:
    """Tests handling when venue is missing."""
    mock_schedule_data: Dict[str, Any] = {
        "content": {
            "schedule": {
                "20231012": {
                    "games": [{
                        "competitions": [{
                            "id": "401547374",
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
    
    mock_get_logo.side_effect = [None, None]
    mock_get_coach.side_effect = [("Andy", "Reid"), ("Sean", "Payton")]
    
    games = get_team_details(2023, 1)
    assert len(games) == 1
    assert games[0]["stadium"] is None

@patch('requests.get')
def test_get_team_details_empty_schedule(mock_requests_get: MagicMock) -> None:
    """Tests handling when schedule is empty."""
    mock_schedule_data: Dict[str, Any] = {
        "content": {
            "schedule": {}
        }
    }
    mock_resp = MagicMock()
    mock_resp.json.return_value = mock_schedule_data
    mock_requests_get.return_value = mock_resp
    
    games = get_team_details(2023, 1)
    assert len(games) == 0

@patch('requests.get')
def test_get_team_details_missing_abbreviation(mock_requests_get: MagicMock) -> None:
    """Tests handling when team abbreviation is missing."""
    mock_schedule_data: Dict[str, Any] = {
        "content": {
            "schedule": {
                "20231012": {
                    "games": [{
                        "competitions": [{
                            "id": "401547374",
                            "venue": {"fullName": "Arrowhead Stadium"},
                            "competitors": [
                                {"homeAway": "home", "team": {"id": "12"}},  # Missing abbreviation
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
    
    games = get_team_details(2023, 1)
    # Should still process the game, but with None for missing abbreviation
    assert len(games) == 1
    assert games[0]["home_team"] is None

# --------------------
# Tests for get_game_date
# --------------------

@patch('requests.get')
def test_get_game_date_success(mock_get: MagicMock) -> None:
    """Tests successful retrieval of game date."""
    mock_event_data = {
        "competitions": [{
            "date": "2025-09-15T17:00Z"
        }]
    }
    mock_response = MagicMock()
    mock_response.json.return_value = mock_event_data
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response
    
    result = get_game_date("401220116")
    assert result == "2025-09-15"
    mock_get.assert_called_once_with("https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401220116", timeout=10)

@patch('requests.get')
def test_get_game_date_success_with_timezone(mock_get: MagicMock) -> None:
    """Tests successful retrieval of game date with timezone."""
    mock_event_data = {
        "competitions": [{
            "date": "2025-09-15T20:30:00+00:00"
        }]
    }
    mock_response = MagicMock()
    mock_response.json.return_value = mock_event_data
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response
    
    result = get_game_date("401220117")
    assert result == "2025-09-15"

@patch('requests.get')
def test_get_game_date_short_date_string(mock_get: MagicMock) -> None:
    """Tests handling when date string is shorter than 10 characters."""
    mock_event_data = {
        "competitions": [{
            "date": "2025-09"
        }]
    }
    mock_response = MagicMock()
    mock_response.json.return_value = mock_event_data
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response
    
    result = get_game_date("401220118")
    assert result is None

@patch('requests.get')
def test_get_game_date_parse_fallback(mock_get: MagicMock) -> None:
    """Tests fallback parsing when ISO format fails."""
    # Use a date string that will fail fromisoformat but is >= 10 chars (triggers fallback)
    mock_event_data = {
        "competitions": [{
            "date": "2025-99-99"  # Invalid date (month > 12) but >= 10 chars
        }]
    }
    mock_response = MagicMock()
    mock_response.json.return_value = mock_event_data
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response
    
    # The function will try fromisoformat which will raise ValueError, then fallback to slicing
    result = get_game_date("401220119")
    assert result == "2025-99-99"  # Fallback returns first 10 chars

@patch('requests.get')
def test_get_game_date_missing_date(mock_get: MagicMock) -> None:
    """Tests handling when date is missing from competition."""
    mock_event_data = {
        "competitions": [{
            # Missing date key
        }]
    }
    mock_response = MagicMock()
    mock_response.json.return_value = mock_event_data
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response
    
    result = get_game_date("401220120")
    assert result is None

@patch('requests.get')
def test_get_game_date_empty_competitions(mock_get: MagicMock) -> None:
    """Tests handling when competitions list is empty."""
    mock_event_data = {
        "competitions": []
    }
    mock_response = MagicMock()
    mock_response.json.return_value = mock_event_data
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response
    
    result = get_game_date("401220121")
    assert result is None

@patch('requests.get')
def test_get_game_date_missing_competitions(mock_get: MagicMock) -> None:
    """Tests handling when competitions key is missing."""
    mock_event_data = {}
    mock_response = MagicMock()
    mock_response.json.return_value = mock_event_data
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response
    
    result = get_game_date("401220122")
    assert result is None

@patch('requests.get')
def test_get_game_date_api_error(mock_get: MagicMock) -> None:
    """Tests handling of API request exception."""
    mock_get.side_effect = requests.exceptions.RequestException("Connection error")
    
    result = get_game_date("401220123")
    assert result is None

@patch('requests.get')
def test_get_game_date_http_error(mock_get: MagicMock) -> None:
    """Tests handling of HTTP error (404, 500, etc.)."""
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Not Found")
    mock_get.return_value = mock_response
    
    result = get_game_date("401220124")
    assert result is None

@patch('requests.get')
def test_get_game_date_parsing_error(mock_get: MagicMock) -> None:
    """Tests handling of parsing errors (KeyError, IndexError, TypeError)."""
    # Test KeyError
    mock_response = MagicMock()
    mock_response.json.side_effect = KeyError("Missing key")
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response
    
    result = get_game_date("401220125")
    assert result is None

@patch('requests.get')
def test_get_game_date_type_error(mock_get: MagicMock) -> None:
    """Tests handling of TypeError in parsing."""
    mock_event_data = {
        "competitions": None  # Should be a list, not None
    }
    mock_response = MagicMock()
    mock_response.json.return_value = mock_event_data
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response
    
    result = get_game_date("401220126")
    assert result is None

@patch('requests.get')
def test_get_game_date_unexpected_error(mock_get: MagicMock) -> None:
    """Tests handling of unexpected exceptions."""
    mock_get.side_effect = Exception("Unexpected error")
    
    result = get_game_date("401220127")
    assert result is None

@patch('requests.get')
def test_get_game_date_null_date(mock_get: MagicMock) -> None:
    """Tests handling when date is None."""
    mock_event_data = {
        "competitions": [{
            "date": None
        }]
    }
    mock_response = MagicMock()
    mock_response.json.return_value = mock_event_data
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response
    
    result = get_game_date("401220128")
    assert result is None

# sync 1774962760885330589
# sync 1774962761966975971
# sync 1774962762102784031
# sync 1774962763684210635
# sync 1774962763702566476
# sync 1774962858808013625
# sync 1774962858836262340
# sync 1774962859633165455
# sys_sync_635b2007
# sys_sync_2860222
# sys_sync_4757dcf
# sys_sync_47284f03
# sys_sync_3859371e
# sys_sync_4b2437d7
