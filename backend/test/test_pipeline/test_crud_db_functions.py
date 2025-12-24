# type: ignore
import pytest
import sqlite3
from unittest.mock import MagicMock
from typing import Dict, Any
import sys
import os

# Add the backend directory to Python path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from app.pipeline.crud_db import (
    insert_prediction_data,
    update_actual_result,
    fetch_predictions,
    fetch_match_scores,
    fetch_prediction_by_game_id,
    fetch_all_predictions,
    update_probabilities
)


class TestCrudDbFunctions:
    """Test cases for crud_db.py functions"""

    @pytest.fixture
    def mock_cursor(self) -> MagicMock:
        """Create a mock database cursor"""
        return MagicMock()

    @pytest.fixture
    def sample_prediction_data(self) -> Dict[str, Any]:
        """Sample prediction data for testing"""
        return {
            'game_id': '401220116',
            'year': 2020,
            'week': 1,
            'home_team': 'BUF',
            'away_team': 'NYJ',
            'home_team_win_probability': 0.65,
            'away_team_win_probability': 0.35,
            'predicted_result': 'BUF',
            'home_team_image_url': 'https://example.com/buf.png',
            'away_team_image_url': 'https://example.com/nyj.png',
            'home_coach': 'Sean McDermott',
            'away_coach': 'Robert Saleh',
            'stadium': 'Highmark Stadium'
        }

    def test_insert_prediction_data_success(self, mock_cursor: MagicMock, sample_prediction_data: Dict[str, Any]) -> None:
        """Test successful insertion of prediction data"""
        insert_query = "INSERT INTO match_predictions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        
        # Call the function
        insert_prediction_data(mock_cursor, insert_query, sample_prediction_data)
        
        # Verify cursor.execute was called with correct parameters
        mock_cursor.execute.assert_called_once_with(insert_query, (
            '401220116',  # game_id
            2020,         # year
            1,            # week
            'BUF',        # home_team
            'NYJ',        # away_team
            0.65,         # home_team_win_probability
            0.35,         # away_team_win_probability
            'BUF',        # predicted_result
            'https://example.com/buf.png',    # home_team_image_url
            'https://example.com/nyj.png',    # away_team_image_url
            'Sean McDermott',  # home_coach
            'Robert Saleh',    # away_coach
            'Highmark Stadium' # stadium
        ))

    def test_insert_prediction_data_missing_key(self, mock_cursor: MagicMock) -> None:
        """Test insert_prediction_data with missing key in prediction_data"""
        incomplete_data = {
            'game_id': '401220116',
            'year': 2020,
            # Missing other required keys
        }
        insert_query = "INSERT INTO match_predictions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        
        with pytest.raises(KeyError):
            insert_prediction_data(mock_cursor, insert_query, incomplete_data)

    def test_insert_prediction_data_empty_dict(self, mock_cursor: MagicMock) -> None:
        """Test insert_prediction_data with empty dictionary"""
        insert_query = "INSERT INTO match_predictions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        
        with pytest.raises(KeyError):
            insert_prediction_data(mock_cursor, insert_query, {})

    def test_update_actual_result_success(self, mock_cursor: MagicMock) -> None:
        """Test successful update of actual result"""
        update_query = "UPDATE match_predictions SET home_score = ?, away_score = ? WHERE game_id = ?"
        game_id = "401220116"
        home_score = 24
        away_score = 17
        
        # Call the function
        update_actual_result(mock_cursor, update_query, game_id, home_score, away_score)
        
        # Verify cursor.execute was called with correct parameters
        mock_cursor.execute.assert_called_once_with(update_query, (24, 17, "401220116"))

    def test_update_actual_result_with_none_scores(self, mock_cursor: MagicMock) -> None:
        """Test update_actual_result with None scores"""
        update_query = "UPDATE match_predictions SET home_score = ?, away_score = ? WHERE game_id = ?"
        game_id = "401220116"
        home_score = None
        away_score = None
        
        # Call the function
        update_actual_result(mock_cursor, update_query, game_id, home_score, away_score)
        
        # Verify cursor.execute was called with None values
        mock_cursor.execute.assert_called_once_with(update_query, (None, None, "401220116"))

    def test_update_actual_result_with_zero_scores(self, mock_cursor: MagicMock) -> None:
        """Test update_actual_result with zero scores"""
        update_query = "UPDATE match_predictions SET home_score = ?, away_score = ? WHERE game_id = ?"
        game_id = "401220116"
        home_score = 0
        away_score = 0
        
        # Call the function
        update_actual_result(mock_cursor, update_query, game_id, home_score, away_score)
        
        # Verify cursor.execute was called with zero values
        mock_cursor.execute.assert_called_once_with(update_query, (0, 0, "401220116"))

    def test_fetch_predictions_success(self, mock_cursor: MagicMock) -> None:
        """Test successful fetch of predictions"""
        fetch_query = "SELECT * FROM match_predictions WHERE year = ? AND week = ?"
        year = 2020
        week = 1
        
        # Mock the fetchall return value
        expected_data = [
            ("401220116", 2020, 1, "BUF", "NYJ", 0.65, 0.35, "BUF", 
             "https://example.com/buf.png", "https://example.com/nyj.png",
             "Sean McDermott", "Robert Saleh", "Highmark Stadium")
        ]
        mock_cursor.fetchall.return_value = expected_data
        
        # Call the function
        result = fetch_predictions(mock_cursor, fetch_query, year, week)
        
        # Verify cursor.execute was called with correct parameters
        mock_cursor.execute.assert_called_once_with(fetch_query, (2020, 1))
        
        # Verify fetchall was called
        mock_cursor.fetchall.assert_called_once()
        
        # Verify the result
        assert result == expected_data

    def test_fetch_predictions_empty_result(self, mock_cursor: MagicMock) -> None:
        """Test fetch_predictions with empty result"""
        fetch_query = "SELECT * FROM match_predictions WHERE year = ? AND week = ?"
        year = 2020
        week = 1
        
        # Mock empty result
        mock_cursor.fetchall.return_value = []
        
        # Call the function
        result = fetch_predictions(mock_cursor, fetch_query, year, week)
        
        # Verify the result is empty
        assert result == []

    def test_fetch_predictions_multiple_results(self, mock_cursor: MagicMock) -> None:
        """Test fetch_predictions with multiple results"""
        fetch_query = "SELECT * FROM match_predictions WHERE year = ? AND week = ?"
        year = 2020
        week = 1
        
        # Mock multiple results
        expected_data = [
            ("401220116", 2020, 1, "BUF", "NYJ", 0.65, 0.35, "BUF", 
             "https://example.com/buf.png", "https://example.com/nyj.png",
             "Sean McDermott", "Robert Saleh", "Highmark Stadium"),
            ("401220117", 2020, 1, "KC", "DEN", 0.70, 0.30, "KC",
             "https://example.com/kc.png", "https://example.com/den.png",
             "Andy Reid", "Sean Payton", "Arrowhead Stadium")
        ]
        mock_cursor.fetchall.return_value = expected_data
        
        # Call the function
        result = fetch_predictions(mock_cursor, fetch_query, year, week)
        
        # Verify the result
        assert result == expected_data
        assert len(result) == 2

    def test_fetch_match_scores_success(self, mock_cursor: MagicMock) -> None:
        """Test successful fetch of match scores"""
        fetch_query = "SELECT home_score, away_score FROM match_scores WHERE game_id = ?"
        game_id = "401220116"
        
        # Mock the fetchall return value
        expected_data = [(24, 17)]
        mock_cursor.fetchall.return_value = expected_data
        
        # Call the function
        result = fetch_match_scores(mock_cursor, fetch_query, game_id)
        
        # Verify cursor.execute was called with correct parameters
        mock_cursor.execute.assert_called_once_with(fetch_query, ("401220116",))
        
        # Verify fetchall was called
        mock_cursor.fetchall.assert_called_once()
        
        # Verify the result
        assert result == expected_data

    def test_fetch_match_scores_empty_result(self, mock_cursor: MagicMock) -> None:
        """Test fetch_match_scores with empty result"""
        fetch_query = "SELECT home_score, away_score FROM match_scores WHERE game_id = ?"
        game_id = "401220116"
        
        # Mock empty result
        mock_cursor.fetchall.return_value = []
        
        # Call the function
        result = fetch_match_scores(mock_cursor, fetch_query, game_id)
        
        # Verify the result is empty
        assert result == []

    def test_fetch_match_scores_with_none_scores(self, mock_cursor: MagicMock) -> None:
        """Test fetch_match_scores with None scores"""
        fetch_query = "SELECT home_score, away_score FROM match_scores WHERE game_id = ?"
        game_id = "401220116"
        
        # Mock result with None scores
        expected_data = [(None, None)]
        mock_cursor.fetchall.return_value = expected_data
        
        # Call the function
        result = fetch_match_scores(mock_cursor, fetch_query, game_id)
        
        # Verify the result
        assert result == expected_data

    def test_fetch_match_scores_multiple_results(self, mock_cursor: MagicMock) -> None:
        """Test fetch_match_scores with multiple results (edge case)"""
        fetch_query = "SELECT home_score, away_score FROM match_scores WHERE game_id = ?"
        game_id = "401220116"
        
        # Mock multiple results (shouldn't happen in practice but testing robustness)
        expected_data = [(24, 17), (21, 14)]
        mock_cursor.fetchall.return_value = expected_data
        
        # Call the function
        result = fetch_match_scores(mock_cursor, fetch_query, game_id)
        
        # Verify the result
        assert result == expected_data

    def test_fetch_prediction_by_game_id_success(self, mock_cursor: MagicMock) -> None:
        """Test successful fetch of prediction by game_id"""
        fetch_query = "SELECT * FROM match_predictions WHERE game_id = ?"
        game_id = "401220116"
        
        # Mock the fetchall return value - full prediction record with all fields
        expected_data = [
            ("401220116", 2020, 1, "BUF", "NYJ", 0.65, 0.35, "BUF",
             "https://example.com/buf.png", "https://example.com/nyj.png",
             "Sean McDermott", "Robert Saleh", "Highmark Stadium", 24, 17)
        ]
        mock_cursor.fetchall.return_value = expected_data
        
        # Call the function
        result = fetch_prediction_by_game_id(mock_cursor, fetch_query, game_id)
        
        # Verify cursor.execute was called with correct parameters
        mock_cursor.execute.assert_called_once_with(fetch_query, ("401220116",))
        
        # Verify fetchall was called
        mock_cursor.fetchall.assert_called_once()
        
        # Verify the result
        assert result == expected_data

    def test_fetch_prediction_by_game_id_empty_result(self, mock_cursor: MagicMock) -> None:
        """Test fetch_prediction_by_game_id with empty result (game not found)"""
        fetch_query = "SELECT * FROM match_predictions WHERE game_id = ?"
        game_id = "401220116"
        
        # Mock empty result
        mock_cursor.fetchall.return_value = []
        
        # Call the function
        result = fetch_prediction_by_game_id(mock_cursor, fetch_query, game_id)
        
        # Verify cursor.execute was called with correct parameters
        mock_cursor.execute.assert_called_once_with(fetch_query, ("401220116",))
        
        # Verify the result is empty
        assert result == []

    def test_fetch_prediction_by_game_id_without_scores(self, mock_cursor: MagicMock) -> None:
        """Test fetch_prediction_by_game_id with prediction that has no scores"""
        fetch_query = "SELECT * FROM match_predictions WHERE game_id = ?"
        game_id = "401220116"
        
        # Mock result without scores (only 13 fields, no home_score/away_score)
        expected_data = [
            ("401220116", 2020, 1, "BUF", "NYJ", 0.65, 0.35, "BUF",
             "https://example.com/buf.png", "https://example.com/nyj.png",
             "Sean McDermott", "Robert Saleh", "Highmark Stadium")
        ]
        mock_cursor.fetchall.return_value = expected_data
        
        # Call the function
        result = fetch_prediction_by_game_id(mock_cursor, fetch_query, game_id)
        
        # Verify the result
        assert result == expected_data
        assert len(result[0]) == 13

    def test_fetch_prediction_by_game_id_multiple_results(self, mock_cursor: MagicMock) -> None:
        """Test fetch_prediction_by_game_id with multiple results (edge case - shouldn't happen)"""
        fetch_query = "SELECT * FROM match_predictions WHERE game_id = ?"
        game_id = "401220116"
        
        # Mock multiple results (shouldn't happen in practice but testing robustness)
        expected_data = [
            ("401220116", 2020, 1, "BUF", "NYJ", 0.65, 0.35, "BUF",
             "https://example.com/buf.png", "https://example.com/nyj.png",
             "Sean McDermott", "Robert Saleh", "Highmark Stadium", 24, 17),
            ("401220116", 2020, 1, "BUF", "NYJ", 0.70, 0.30, "BUF",
             "https://example.com/buf.png", "https://example.com/nyj.png",
             "Sean McDermott", "Robert Saleh", "Highmark Stadium", 21, 14)
        ]
        mock_cursor.fetchall.return_value = expected_data
        
        # Call the function
        result = fetch_prediction_by_game_id(mock_cursor, fetch_query, game_id)
        
        # Verify the result
        assert result == expected_data
        assert len(result) == 2

    def test_fetch_prediction_by_game_id_database_error(self, mock_cursor: MagicMock) -> None:
        """Test fetch_prediction_by_game_id with database error"""
        fetch_query = "SELECT * FROM match_predictions WHERE game_id = ?"
        
        # Mock database error
        mock_cursor.execute.side_effect = sqlite3.Error("Table does not exist")
        
        # Call the function and expect the error to be propagated
        with pytest.raises(sqlite3.Error, match="Table does not exist"):
            _ = fetch_prediction_by_game_id(mock_cursor, fetch_query, "401220116")

    def test_fetch_prediction_by_game_id_with_empty_string(self, mock_cursor: MagicMock) -> None:
        """Test fetch_prediction_by_game_id with empty string game_id"""
        fetch_query = "SELECT * FROM match_predictions WHERE game_id = ?"
        game_id = ""
        
        # Mock empty result
        mock_cursor.fetchall.return_value = []
        
        # Call the function
        result = fetch_prediction_by_game_id(mock_cursor, fetch_query, game_id)
        
        # Verify cursor.execute was called with empty string
        mock_cursor.execute.assert_called_once_with(fetch_query, ("",))
        assert result == []

    def test_fetch_prediction_by_game_id_with_special_characters(self, mock_cursor: MagicMock) -> None:
        """Test fetch_prediction_by_game_id with special characters in game_id"""
        fetch_query = "SELECT * FROM match_predictions WHERE game_id = ?"
        game_id = "401220116_2023-09-10"
        
        # Mock result
        expected_data = [
            ("401220116_2023-09-10", 2020, 1, "BUF", "NYJ", 0.65, 0.35, "BUF",
             "https://example.com/buf.png", "https://example.com/nyj.png",
             "Sean McDermott", "Robert Saleh", "Highmark Stadium")
        ]
        mock_cursor.fetchall.return_value = expected_data
        
        # Call the function
        result = fetch_prediction_by_game_id(mock_cursor, fetch_query, game_id)
        
        # Verify cursor.execute was called with special characters
        mock_cursor.execute.assert_called_once_with(fetch_query, ("401220116_2023-09-10",))
        assert result == expected_data

    def test_database_error_handling(self, mock_cursor: MagicMock) -> None:
        """Test that database errors are propagated correctly"""
        # Test insert_prediction_data with database error
        insert_query = "INSERT INTO match_predictions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        sample_data = {
            'game_id': '401220116',
            'year': 2020,
            'week': 1,
            'home_team': 'BUF',
            'away_team': 'NYJ',
            'home_team_win_probability': 0.65,
            'away_team_win_probability': 0.35,
            'predicted_result': 'BUF',
            'home_team_image_url': 'https://example.com/buf.png',
            'away_team_image_url': 'https://example.com/nyj.png',
            'home_coach': 'Sean McDermott',
            'away_coach': 'Robert Saleh',
            'stadium': 'Highmark Stadium'
        }
        
        # Mock database error
        mock_cursor.execute.side_effect = sqlite3.Error("Database constraint violation")
        
        # Call the function and expect the error to be propagated
        with pytest.raises(sqlite3.Error, match="Database constraint violation"):
            insert_prediction_data(mock_cursor, insert_query, sample_data)

    def test_fetch_predictions_database_error(self, mock_cursor: MagicMock) -> None:
        """Test fetch_predictions with database error"""
        fetch_query = "SELECT * FROM match_predictions WHERE year = ? AND week = ?"
        
        # Mock database error
        mock_cursor.execute.side_effect = sqlite3.Error("Database connection lost")
        
        # Call the function and expect the error to be propagated
        with pytest.raises(sqlite3.Error, match="Database connection lost"):
            _ = fetch_predictions(mock_cursor, fetch_query, 2020, 1)

    def test_fetch_match_scores_database_error(self, mock_cursor: MagicMock) -> None:
        """Test fetch_match_scores with database error"""
        fetch_query = "SELECT home_score, away_score FROM match_scores WHERE game_id = ?"
        
        # Mock database error
        mock_cursor.execute.side_effect = sqlite3.Error("Table does not exist")
        
        # Call the function and expect the error to be propagated
        with pytest.raises(sqlite3.Error, match="Table does not exist"):
            _ = fetch_match_scores(mock_cursor, fetch_query, "401220116")

    def test_update_actual_result_database_error(self, mock_cursor: MagicMock) -> None:
        """Test update_actual_result with database error"""
        update_query = "UPDATE match_predictions SET home_score = ?, away_score = ? WHERE game_id = ?"
        
        # Mock database error
        mock_cursor.execute.side_effect = sqlite3.Error("Constraint violation")
        
        # Call the function and expect the error to be propagated
        with pytest.raises(sqlite3.Error, match="Constraint violation"):
            update_actual_result(mock_cursor, update_query, "401220116", 24, 17)

    def test_function_parameter_types(self, mock_cursor: MagicMock) -> None:
        """Test functions with different parameter types"""
        # Test with string parameters
        fetch_query = "SELECT * FROM match_predictions WHERE year = ? AND week = ?"
        _ = fetch_predictions(mock_cursor, fetch_query, 2020, 1)
        mock_cursor.execute.assert_called_with(fetch_query, (2020, 1))
        
        # Test with integer parameters
        mock_cursor.reset_mock()
        _ = fetch_predictions(mock_cursor, fetch_query, 2020, 1)
        mock_cursor.execute.assert_called_with(fetch_query, (2020, 1))

    def test_edge_case_empty_strings(self, mock_cursor: MagicMock) -> None:
        """Test functions with empty string parameters"""
        # Test fetch_match_scores with empty game_id
        fetch_query = "SELECT home_score, away_score FROM match_scores WHERE game_id = ?"
        _ = fetch_match_scores(mock_cursor, fetch_query, "")
        mock_cursor.execute.assert_called_with(fetch_query, ("",))
        
        # Test update_actual_result with empty game_id
        update_query = "UPDATE match_predictions SET home_score = ?, away_score = ? WHERE game_id = ?"
        update_actual_result(mock_cursor, update_query, "", 0, 0)
        mock_cursor.execute.assert_called_with(update_query, (0, 0, ""))

    def test_large_numbers(self, mock_cursor: MagicMock) -> None:
        """Test functions with large numbers"""
        # Test with large year and week
        fetch_query = "SELECT * FROM match_predictions WHERE year = ? AND week = ?"
        _ = fetch_predictions(mock_cursor, fetch_query, 9999, 99)
        mock_cursor.execute.assert_called_with(fetch_query, (9999, 99))
        
        # Test with large scores
        update_query = "UPDATE match_predictions SET home_score = ?, away_score = ? WHERE game_id = ?"
        update_actual_result(mock_cursor, update_query, "401220116", 999, 999)
        mock_cursor.execute.assert_called_with(update_query, (999, 999, "401220116"))

    def test_fetch_all_predictions_success(self, mock_cursor: MagicMock) -> None:
        """Test successful fetch of all predictions"""
        fetch_query = "SELECT game_id, year, week, home_team, away_team, home_coach, away_coach, stadium FROM match_predictions"
        
        # Mock the fetchall return value
        expected_data = [
            ("401220116", 2020, 1, "BUF", "NYJ", "Sean McDermott", "Robert Saleh", "Highmark Stadium"),
            ("401220117", 2020, 1, "KC", "DEN", "Andy Reid", "Sean Payton", "Arrowhead Stadium"),
            ("401220118", 2020, 2, "SF", "LAR", "Kyle Shanahan", "Sean McVay", "Levi's Stadium")
        ]
        mock_cursor.fetchall.return_value = expected_data
        
        # Call the function
        result = fetch_all_predictions(mock_cursor, fetch_query)
        
        # Verify cursor.execute was called with correct parameters (no parameters for this query)
        mock_cursor.execute.assert_called_once_with(fetch_query)
        
        # Verify fetchall was called
        mock_cursor.fetchall.assert_called_once()
        
        # Verify the result
        assert result == expected_data
        assert len(result) == 3

    def test_fetch_all_predictions_empty_result(self, mock_cursor: MagicMock) -> None:
        """Test fetch_all_predictions with empty result"""
        fetch_query = "SELECT game_id, year, week, home_team, away_team, home_coach, away_coach, stadium FROM match_predictions"
        
        # Mock empty result
        mock_cursor.fetchall.return_value = []
        
        # Call the function
        result = fetch_all_predictions(mock_cursor, fetch_query)
        
        # Verify the result is empty
        assert result == []
        mock_cursor.execute.assert_called_once_with(fetch_query)

    def test_fetch_all_predictions_single_result(self, mock_cursor: MagicMock) -> None:
        """Test fetch_all_predictions with single result"""
        fetch_query = "SELECT game_id, year, week, home_team, away_team, home_coach, away_coach, stadium FROM match_predictions"
        
        # Mock single result
        expected_data = [
            ("401220116", 2020, 1, "BUF", "NYJ", "Sean McDermott", "Robert Saleh", "Highmark Stadium")
        ]
        mock_cursor.fetchall.return_value = expected_data
        
        # Call the function
        result = fetch_all_predictions(mock_cursor, fetch_query)
        
        # Verify the result
        assert result == expected_data
        assert len(result) == 1

    def test_fetch_all_predictions_database_error(self, mock_cursor: MagicMock) -> None:
        """Test fetch_all_predictions with database error"""
        fetch_query = "SELECT game_id, year, week, home_team, away_team, home_coach, away_coach, stadium FROM match_predictions"
        
        # Mock database error
        mock_cursor.execute.side_effect = sqlite3.Error("Table does not exist")
        
        # Call the function and expect the error to be propagated
        with pytest.raises(sqlite3.Error, match="Table does not exist"):
            _ = fetch_all_predictions(mock_cursor, fetch_query)

    def test_update_probabilities_success(self, mock_cursor: MagicMock) -> None:
        """Test successful update of probabilities"""
        update_query = "UPDATE match_predictions SET home_team_win_probability = ?, away_team_win_probability = ?, predicted_result = ? WHERE game_id = ?"
        game_id = "401220116"
        home_prob = 0.75
        away_prob = 0.25
        predicted_result = "BUF"
        
        # Call the function
        update_probabilities(mock_cursor, update_query, game_id, home_prob, away_prob, predicted_result,
                             "BUF", "NYJ", "Sean McDermott", "Robert Saleh", "Highmark Stadium",
                             "https://example.com/buf.png", "https://example.com/nyj.png")
        
        # Verify cursor.execute was called with correct parameters
        mock_cursor.execute.assert_called_once_with(update_query, (
            0.75, 0.25, "BUF", "BUF", "NYJ", "Sean McDermott", "Robert Saleh", 
            "Highmark Stadium", "https://example.com/buf.png", "https://example.com/nyj.png", 
            "401220116"
        ))

    def test_update_probabilities_with_equal_probabilities(self, mock_cursor: MagicMock) -> None:
        """Test update_probabilities with equal probabilities"""
        update_query = "UPDATE match_predictions SET home_team_win_probability = ?, away_team_win_probability = ?, predicted_result = ? WHERE game_id = ?"
        game_id = "401220116"
        home_prob = 0.5
        away_prob = 0.5
        predicted_result = "TIE"
        
        # Call the function
        update_probabilities(mock_cursor, update_query, game_id, home_prob, away_prob, predicted_result,
                             "BUF", "NYJ", "Sean McDermott", "Robert Saleh", "Highmark Stadium",
                             "https://example.com/buf.png", "https://example.com/nyj.png")
        
        # Verify cursor.execute was called with equal probabilities
        mock_cursor.execute.assert_called_once_with(update_query, (
            0.5, 0.5, "TIE", "BUF", "NYJ", "Sean McDermott", "Robert Saleh", 
            "Highmark Stadium", "https://example.com/buf.png", "https://example.com/nyj.png", 
            "401220116"
        ))

    def test_update_probabilities_with_extreme_values(self, mock_cursor: MagicMock) -> None:
        """Test update_probabilities with extreme probability values"""
        update_query = "UPDATE match_predictions SET home_team_win_probability = ?, away_team_win_probability = ?, predicted_result = ? WHERE game_id = ?"
        game_id = "401220116"
        home_prob = 0.99
        away_prob = 0.01
        predicted_result = "BUF"
        
        # Call the function
        update_probabilities(mock_cursor, update_query, game_id, home_prob, away_prob, predicted_result,
                             "BUF", "NYJ", "Sean McDermott", "Robert Saleh", "Highmark Stadium",
                             "https://example.com/buf.png", "https://example.com/nyj.png")
        
        # Verify cursor.execute was called with extreme values
        mock_cursor.execute.assert_called_once_with(update_query, (
            0.99, 0.01, "BUF", "BUF", "NYJ", "Sean McDermott", "Robert Saleh", 
            "Highmark Stadium", "https://example.com/buf.png", "https://example.com/nyj.png", 
            "401220116"
        ))

    def test_update_probabilities_with_zero_probabilities(self, mock_cursor: MagicMock) -> None:
        """Test update_probabilities with zero probabilities"""
        update_query = "UPDATE match_predictions SET home_team_win_probability = ?, away_team_win_probability = ?, predicted_result = ? WHERE game_id = ?"
        game_id = "401220116"
        home_prob = 0.0
        away_prob = 1.0
        predicted_result = "NYJ"
        
        # Call the function
        update_probabilities(mock_cursor, update_query, game_id, home_prob, away_prob, predicted_result,
                             "BUF", "NYJ", "Sean McDermott", "Robert Saleh", "Highmark Stadium",
                             "https://example.com/buf.png", "https://example.com/nyj.png")
        
        # Verify cursor.execute was called with zero and one probabilities
        mock_cursor.execute.assert_called_once_with(update_query, (
            0.0, 1.0, "NYJ", "BUF", "NYJ", "Sean McDermott", "Robert Saleh", 
            "Highmark Stadium", "https://example.com/buf.png", "https://example.com/nyj.png", 
            "401220116"
        ))

    def test_update_probabilities_with_empty_string_result(self, mock_cursor: MagicMock) -> None:
        """Test update_probabilities with empty string predicted_result"""
        update_query = "UPDATE match_predictions SET home_team_win_probability = ?, away_team_win_probability = ?, predicted_result = ? WHERE game_id = ?"
        game_id = "401220116"
        home_prob = 0.6
        away_prob = 0.4
        predicted_result = ""
        
        # Call the function
        update_probabilities(mock_cursor, update_query, game_id, home_prob, away_prob, predicted_result,
                             "BUF", "NYJ", "Sean McDermott", "Robert Saleh", "Highmark Stadium",
                             "https://example.com/buf.png", "https://example.com/nyj.png")
        
        # Verify cursor.execute was called with empty string
        mock_cursor.execute.assert_called_once_with(update_query, (
            0.6, 0.4, "", "BUF", "NYJ", "Sean McDermott", "Robert Saleh", 
            "Highmark Stadium", "https://example.com/buf.png", "https://example.com/nyj.png", 
            "401220116"
        ))

    def test_update_probabilities_with_long_team_name(self, mock_cursor: MagicMock) -> None:
        """Test update_probabilities with long team name in predicted_result"""
        update_query = "UPDATE match_predictions SET home_team_win_probability = ?, away_team_win_probability = ?, predicted_result = ? WHERE game_id = ?"
        game_id = "401220116"
        home_prob = 0.7
        away_prob = 0.3
        predicted_result = "New England Patriots"
        
        # Call the function
        update_probabilities(mock_cursor, update_query, game_id, home_prob, away_prob, predicted_result,
                             "BUF", "NYJ", "Sean McDermott", "Robert Saleh", "Highmark Stadium",
                             "https://example.com/buf.png", "https://example.com/nyj.png")
        
        # Verify cursor.execute was called with long team name
        mock_cursor.execute.assert_called_once_with(update_query, (
            0.7, 0.3, "New England Patriots", "BUF", "NYJ", "Sean McDermott", 
            "Robert Saleh", "Highmark Stadium", "https://example.com/buf.png", "https://example.com/nyj.png", 
            "401220116"
        ))

    def test_update_probabilities_database_error(self, mock_cursor: MagicMock) -> None:
        """Test update_probabilities with database error"""
        update_query = "UPDATE match_predictions SET home_team_win_probability = ?, away_team_win_probability = ?, predicted_result = ? WHERE game_id = ?"
        
        # Mock database error
        mock_cursor.execute.side_effect = sqlite3.Error("Constraint violation")
        
        # Call the function and expect the error to be propagated
        with pytest.raises(sqlite3.Error, match="Constraint violation"):
            update_probabilities(mock_cursor, update_query, "401220116", 0.65, 0.35, "BUF",
                                 "BUF", "NYJ", "Sean McDermott", "Robert Saleh", "Highmark Stadium",
                                 "https://example.com/buf.png", "https://example.com/nyj.png")

    def test_update_probabilities_with_special_characters(self, mock_cursor: MagicMock) -> None:
        """Test update_probabilities with special characters in game_id"""
        update_query = "UPDATE match_predictions SET home_team_win_probability = ?, away_team_win_probability = ?, predicted_result = ? WHERE game_id = ?"
        game_id = "401220116_2023-09-10"
        home_prob = 0.65
        away_prob = 0.35
        predicted_result = "BUF"
        
        # Call the function
        # Call the function
        update_probabilities(mock_cursor, update_query, game_id, home_prob, away_prob, predicted_result,
                             "BUF", "NYJ", "Sean McDermott", "Robert Saleh", "Highmark Stadium",
                             "https://example.com/buf.png", "https://example.com/nyj.png")
        
        # Verify cursor.execute was called with special characters in game_id
        # Verify cursor.execute was called with special characters in game_id
        mock_cursor.execute.assert_called_once_with(update_query, (
            0.65, 0.35, "BUF", "BUF", "NYJ", "Sean McDermott", "Robert Saleh", 
            "Highmark Stadium", "https://example.com/buf.png", "https://example.com/nyj.png", 
            "401220116_2023-09-10"
        ))
# sync 1774962762120615497
# sync 1774962858261711159
# sync 1774962859069707176
# sys_sync_1b0240d5
# sys_sync_7bd54a7a
# sys_sync_56516140
