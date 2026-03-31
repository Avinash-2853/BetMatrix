# type: ignore
import pytest
import sqlite3
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient
from fastapi import FastAPI
import sys
import os

# Add the backend directory to Python path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from app.api.fetch_data_api.services import FetchDataService
from app.api.fetch_data_api.routes import router


class TestFetchDataService:
    """Test cases for FetchDataService class"""

    @pytest.fixture
    def mock_env(self):
        """Mock environment variables"""
        with patch.dict(os.environ, {'database_path': '/test/database.db'}):
            yield

    @pytest.fixture
    def service(self, mock_env):
        """Create FetchDataService instance with mocked dependencies"""
        with patch('app.api.fetch_data_api.services.sqlite3.connect') as mock_connect:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_conn
            
            service = FetchDataService()
            service._mock_conn = mock_conn
            service._mock_cursor = mock_cursor
            yield service

    def test_init_without_database_path(self):
        """Test service initialization uses default database path when not set"""
        with patch.dict(os.environ, {}, clear=True):
            service = FetchDataService()
            # Should use default database path
            expected_path = os.path.join(
                os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")),
                "data", "database", "nfl_predictions.db"
            )
            assert service.database_path == expected_path

    def test_get_database_connection_success(self, service):
        """Test successful database connection"""
        with patch('app.api.fetch_data_api.services.sqlite3.connect') as mock_connect:
            mock_conn = MagicMock()
            mock_connect.return_value = mock_conn
            
            conn = service.get_database_connection()
            
            assert conn == mock_conn
            mock_connect.assert_called_once_with(service.database_path)

    def test_get_database_connection_failure(self, service):
        """Test database connection failure"""
        with patch('app.api.fetch_data_api.services.sqlite3.connect') as mock_connect:
            mock_connect.side_effect = sqlite3.Error("Connection failed")
            
            with pytest.raises(Exception, match="Database connection failed: Connection failed"):
                service.get_database_connection()

    def test_fetch_predictions_by_year_week_invalid_year(self, service):
        """Test fetch_predictions_by_year_week with invalid year"""
        # Test year too low
        with pytest.raises(ValueError, match="Year must be an integer between 2000 and 2030"):
            service.fetch_predictions_by_year_week(1999, 1)
        
        # Test year too high
        with pytest.raises(ValueError, match="Year must be an integer between 2000 and 2030"):
            service.fetch_predictions_by_year_week(2031, 1)

    def test_fetch_predictions_by_year_week_invalid_week(self, service):
        """Test fetch_predictions_by_year_week with invalid week"""
        # Test week too low
        with pytest.raises(ValueError, match="Week must be an integer between 1 and 53"):
            service.fetch_predictions_by_year_week(2020, 0)
        
        # Test week too high
        with pytest.raises(ValueError, match="Week must be an integer between 1 and 53"):
            service.fetch_predictions_by_year_week(2020, 54)

    @patch('app.api.fetch_data_api.services.fetch_predictions')
    def test_fetch_predictions_by_year_week_success(self, mock_fetch_predictions, service):
        """Test successful fetch_predictions_by_year_week"""
        # Mock data
        mock_raw_data = [
            ("401220116", 2020, 1, "BUF", "NYJ", 0.65, 0.35, "BUF", 
             "https://example.com/buf.png", "https://example.com/nyj.png",
             "Sean McDermott", "Robert Saleh", "Highmark Stadium")
        ]
        mock_fetch_predictions.return_value = mock_raw_data
        
        # Call method
        result = service.fetch_predictions_by_year_week(2020, 1)
        
        # Assertions
        assert len(result) == 1
        prediction = result[0]
        assert prediction["game_id"] == "401220116"
        assert prediction["year"] == 2020
        assert prediction["week"] == 1
        assert prediction["home_team"] == "BUF"
        assert prediction["away_team"] == "NYJ"
        assert prediction["home_team_win_probability"] == pytest.approx(0.65)
        assert prediction["away_team_win_probability"] == pytest.approx(0.35)
        assert prediction["predicted_result"] == "BUF"
        assert prediction["home_team_image_url"] == "https://example.com/buf.png"
        assert prediction["away_team_image_url"] == "https://example.com/nyj.png"
        assert prediction["home_coach"] == "Sean McDermott"
        assert prediction["away_coach"] == "Robert Saleh"
        assert prediction["stadium"] == "Highmark Stadium"
        
        # Verify that fetch_predictions was called with correct parameters
        from app.core.query import fetch_data_query
        mock_fetch_predictions.assert_called_once_with(
            service._mock_cursor, 
            fetch_data_query, 
            2020, 
            1
        )
        service._mock_conn.close.assert_called_once()

    @patch('app.api.fetch_data_api.services.fetch_predictions')
    def test_fetch_predictions_by_year_week_database_error(self, mock_fetch_predictions, service):
        """Test fetch_predictions_by_year_week with database error"""
        mock_fetch_predictions.side_effect = sqlite3.Error("Database error")
        
        with pytest.raises(Exception, match="Database query failed: Database error"):
            service.fetch_predictions_by_year_week(2020, 1)

    @patch('app.api.fetch_data_api.services.fetch_predictions')
    def test_fetch_predictions_by_year_week_runtime_error(self, mock_fetch_predictions, service):
        """Test fetch_predictions_by_year_week with unexpected runtime error"""
        mock_fetch_predictions.side_effect = RuntimeError("Unexpected failure")
        
        with pytest.raises(RuntimeError, match="Failed to fetch predictions: Unexpected failure"):
            service.fetch_predictions_by_year_week(2020, 1)

    def test_get_available_years_weeks_success(self, service):
        """Test successful get_available_years_weeks"""
        # Mock cursor responses
        service._mock_cursor.fetchall.side_effect = [
            [(2020,), (2021,), (2022,)],  # years
            [(1,), (2,), (3,), (4,)]      # weeks
        ]
        
        result = service.get_available_years_weeks()
        
        assert result["years"] == [2020, 2021, 2022]
        assert result["weeks"] == [1, 2, 3, 4]
        
        # Verify database operations
        assert service._mock_cursor.execute.call_count == 2
        service._mock_cursor.execute.assert_any_call("SELECT DISTINCT year FROM match_predictions ORDER BY year")
        service._mock_cursor.execute.assert_any_call("SELECT DISTINCT week FROM match_predictions ORDER BY week")
        service._mock_conn.close.assert_called_once()

    def test_get_available_years_weeks_database_error(self, service):
        """Test get_available_years_weeks with database error"""
        service._mock_cursor.execute.side_effect = sqlite3.Error("Database error")
        
        with pytest.raises(Exception, match="Database query failed: Database error"):
            service.get_available_years_weeks()

    def test_get_available_years_weeks_runtime_error(self, service):
        """Test get_available_years_weeks with unexpected runtime error"""
        service._mock_cursor.execute.side_effect = RuntimeError("Unexpected failure")
        
        with pytest.raises(RuntimeError, match="Failed to get available years and weeks: Unexpected failure"):
            service.get_available_years_weeks()

    @patch('app.api.fetch_data_api.services.FetchDataService.fetch_predictions_by_year_week')
    def test_get_prediction_summary_success(self, mock_fetch_predictions, service):
        """Test successful get_prediction_summary"""
        # Mock predictions data
        mock_predictions = [
            {
                "game_id": "401220116",
                "home_team": "BUF",
                "away_team": "NYJ",
                "predicted_result": "BUF",
                "home_team_win_probability": 0.65,
                "away_team_win_probability": 0.35
            },
            {
                "game_id": "401220117",
                "home_team": "KC",
                "away_team": "DEN",
                "predicted_result": "KC",
                "home_team_win_probability": 0.70,
                "away_team_win_probability": 0.30
            }
        ]
        mock_fetch_predictions.return_value = mock_predictions
        
        result = service.get_prediction_summary(2020, 1)
        
        assert result["total_games"] == 2
        assert result["year"] == 2020
        assert result["week"] == 1
        assert result["predicted_home_wins"] == 2
        assert result["predicted_away_wins"] == 0
        assert result["average_home_win_probability"] == pytest.approx(0.675)
        assert result["average_away_win_probability"] == pytest.approx(0.325)
        assert result["predictions"] == mock_predictions

    @patch('app.api.fetch_data_api.services.FetchDataService.fetch_predictions_by_year_week')
    def test_get_prediction_summary_no_predictions(self, mock_fetch_predictions, service):
        """Test get_prediction_summary with no predictions"""
        mock_fetch_predictions.return_value = []
        
        result = service.get_prediction_summary(2020, 1)
        
        assert result["total_games"] == 0
        assert result["year"] == 2020
        assert result["week"] == 1
        assert result["predicted_home_wins"] == 0
        assert result["predicted_away_wins"] == 0
        assert result["average_home_win_probability"] == 0.0
        assert result["average_away_win_probability"] == 0.0
        assert result["predictions"] == []

    def test_fetch_prediction_by_game_id_invalid_game_id(self, service):
        """Test fetch_prediction_by_game_id with invalid game_id"""
        # Test empty string
        with pytest.raises(ValueError, match="Game ID must be a non-empty string"):
            service.fetch_prediction_by_game_id("")
        
        # Test whitespace only
        with pytest.raises(ValueError, match="Game ID must be a non-empty string"):
            service.fetch_prediction_by_game_id("   ")

    @patch('app.api.fetch_data_api.services.fetch_prediction_by_game_id')
    def test_fetch_prediction_by_game_id_success(self, mock_fetch_prediction, service):
        """Test successful fetch_prediction_by_game_id"""
        # Mock data with all fields including scores
        mock_raw_data = [
            ("401220116", 2020, 1, "BUF", "NYJ", 0.65, 0.35, "BUF",
             "https://example.com/buf.png", "https://example.com/nyj.png",
             "Sean McDermott", "Robert Saleh", "Highmark Stadium", 24, 17)
        ]
        mock_fetch_prediction.return_value = mock_raw_data
        
        result = service.fetch_prediction_by_game_id("401220116")
        
        assert result["game_id"] == "401220116"
        assert result["year"] == 2020
        assert result["week"] == 1
        assert result["home_team"] == "BUF"
        assert result["away_team"] == "NYJ"
        assert result["home_team_win_probability"] == pytest.approx(0.65)
        assert result["away_team_win_probability"] == pytest.approx(0.35)
        assert result["predicted_result"] == "BUF"
        assert result["home_team_image_url"] == "https://example.com/buf.png"
        assert result["away_team_image_url"] == "https://example.com/nyj.png"
        assert result["home_coach"] == "Sean McDermott"
        assert result["away_coach"] == "Robert Saleh"
        assert result["stadium"] == "Highmark Stadium"
        assert result["home_score"] == 24
        assert result["away_score"] == 17
        
        # Verify that fetch_prediction_by_game_id was called with correct parameters
        from app.core.query import fetch_prediction_by_game_id_query
        mock_fetch_prediction.assert_called_once_with(
            service._mock_cursor, 
            fetch_prediction_by_game_id_query, 
            "401220116"
        )
        service._mock_conn.close.assert_called_once()

    @patch('app.api.fetch_data_api.services.fetch_prediction_by_game_id')
    def test_fetch_prediction_by_game_id_not_found(self, mock_fetch_prediction, service):
        """Test fetch_prediction_by_game_id with no results"""
        mock_fetch_prediction.return_value = []
        
        # ValueError is caught by generic Exception handler and re-raised as RuntimeError
        with pytest.raises(RuntimeError, match="Failed to fetch prediction: No prediction found for game_id: 401220116"):
            service.fetch_prediction_by_game_id("401220116")

    @patch('app.api.fetch_data_api.services.fetch_prediction_by_game_id')
    def test_fetch_prediction_by_game_id_database_error(self, mock_fetch_prediction, service):
        """Test fetch_prediction_by_game_id with database error"""
        mock_fetch_prediction.side_effect = sqlite3.Error("Database error")
        
        with pytest.raises(sqlite3.DatabaseError, match="Database query failed: Database error"):
            service.fetch_prediction_by_game_id("401220116")

    @patch('app.api.fetch_data_api.services.fetch_prediction_by_game_id')
    def test_fetch_prediction_by_game_id_runtime_error(self, mock_fetch_prediction, service):
        """Test fetch_prediction_by_game_id with unexpected runtime error"""
        mock_fetch_prediction.side_effect = RuntimeError("Unexpected failure")
        
        with pytest.raises(RuntimeError, match="Failed to fetch prediction: Unexpected failure"):
            service.fetch_prediction_by_game_id("401220116")

    @patch('app.api.fetch_data_api.services.fetch_prediction_by_game_id')
    def test_fetch_prediction_by_game_id_without_scores(self, mock_fetch_prediction, service):
        """Test fetch_prediction_by_game_id with prediction that has no scores"""
        # Mock data without scores (only 13 fields)
        mock_raw_data = [
            ("401220116", 2020, 1, "BUF", "NYJ", 0.65, 0.35, "BUF",
             "https://example.com/buf.png", "https://example.com/nyj.png",
             "Sean McDermott", "Robert Saleh", "Highmark Stadium")
        ]
        mock_fetch_prediction.return_value = mock_raw_data
        
        result = service.fetch_prediction_by_game_id("401220116")
        
        assert result["game_id"] == "401220116"
        assert result["home_score"] is None
        assert result["away_score"] is None

    def test_fetch_match_scores_by_game_id_invalid_game_id(self, service):
        """Test fetch_match_scores_by_game_id with invalid game_id"""
        # Test empty string
        with pytest.raises(ValueError, match="Game ID must be a non-empty string"):
            service.fetch_match_scores_by_game_id("")
        
        # Test whitespace only
        with pytest.raises(ValueError, match="Game ID must be a non-empty string"):
            service.fetch_match_scores_by_game_id("   ")

    @patch('app.api.fetch_data_api.services.fetch_match_scores')
    def test_fetch_match_scores_by_game_id_success(self, mock_fetch_match_scores, service):
        """Test successful fetch_match_scores_by_game_id"""
        # Mock data
        mock_raw_data = [("401220116", 24, 17)]
        mock_fetch_match_scores.return_value = mock_raw_data
        
        result = service.fetch_match_scores_by_game_id("401220116")
        
        assert result["game_id"] == "401220116"
        assert result["home_score"] == 24
        assert result["away_score"] == 17
        
        # Verify that fetch_match_scores was called with correct parameters
        from app.core.query import fetch_match_scores_query
        mock_fetch_match_scores.assert_called_once_with(
            service._mock_cursor, 
            fetch_match_scores_query, 
            "401220116"
        )
        service._mock_conn.close.assert_called_once()

    @patch('app.api.fetch_data_api.services.fetch_match_scores')
    def test_fetch_match_scores_by_game_id_not_found(self, mock_fetch_match_scores, service):
        """Test fetch_match_scores_by_game_id with no results"""
        mock_fetch_match_scores.return_value = []
        
        with pytest.raises(Exception, match="Failed to fetch match scores: No match scores found for game_id: 401220116"):
            service.fetch_match_scores_by_game_id("401220116")

    @patch('app.api.fetch_data_api.services.fetch_match_scores')
    def test_fetch_match_scores_by_game_id_database_error(self, mock_fetch_match_scores, service):
        """Test fetch_match_scores_by_game_id with database error"""
        mock_fetch_match_scores.side_effect = sqlite3.Error("Database error")
        
        with pytest.raises(Exception, match="Database query failed: Database error"):
            service.fetch_match_scores_by_game_id("401220116")


class TestFetchDataRoutes:
    """Test cases for fetch_data_api routes"""

    @pytest.fixture
    def app(self):
        """Create FastAPI app with router"""
        app = FastAPI()
        app.include_router(router)
        return app

    @pytest.fixture
    def client(self, app):
        """Create test client"""
        return TestClient(app)

    @pytest.fixture
    def mock_service(self):
        """Mock FetchDataService"""
        with patch('app.api.fetch_data_api.routes.fetch_service') as mock_service:
            yield mock_service

    def test_get_predictions_success(self, client, mock_service):
        """Test successful get_predictions endpoint"""
        # Mock service response
        mock_predictions = [
            {
                "game_id": "401220116",
                "year": 2020,
                "week": 1,
                "home_team": "BUF",
                "away_team": "NYJ",
                "home_team_win_probability": 0.65,
                "away_team_win_probability": 0.35,
                "predicted_result": "BUF",
                "home_team_image_url": "https://example.com/buf.png",
                "away_team_image_url": "https://example.com/nyj.png",
                "home_coach": "Sean McDermott",
                "away_coach": "Robert Saleh",
                "stadium": "Highmark Stadium"
            }
        ]
        mock_service.fetch_predictions_by_year_week.return_value = mock_predictions
        
        response = client.get("/api/fetch-data/predictions?year=2020&week=1")
        
        assert response.status_code == 200
        data = response.json()
        assert data["total_count"] == 1
        assert data["year"] == 2020
        assert data["week"] == 1
        assert len(data["predictions"]) == 1
        assert data["predictions"][0]["game_id"] == "401220116"

    def test_get_predictions_invalid_year(self, client, mock_service):
        """Test get_predictions with invalid year"""
        mock_service.fetch_predictions_by_year_week.side_effect = ValueError("Year must be an integer between 2000 and 2030")
        
        response = client.get("/api/fetch-data/predictions?year=1999&week=1")
        
        assert response.status_code == 400
        assert "Year must be an integer between 2000 and 2030" in response.json()["detail"]

    def test_get_predictions_server_error(self, client, mock_service):
        """Test get_predictions with server error"""
        mock_service.fetch_predictions_by_year_week.side_effect = Exception("Database connection failed")
        
        response = client.get("/api/fetch-data/predictions?year=2020&week=1")
        
        assert response.status_code == 500
        assert "Internal server error" in response.json()["detail"]

    def test_get_prediction_summary_success(self, client, mock_service):
        """Test successful get_prediction_summary endpoint"""
        # Mock service response
        mock_summary = {
            "total_games": 2,
            "year": 2020,
            "week": 1,
            "predicted_home_wins": 2,
            "predicted_away_wins": 0,
            "average_home_win_probability": 0.675,
            "average_away_win_probability": 0.325,
            "predictions": [
                {
                    "game_id": "401220116",
                    "year": 2020,
                    "week": 1,
                    "home_team": "BUF",
                    "away_team": "NYJ",
                    "home_team_win_probability": 0.65,
                    "away_team_win_probability": 0.35,
                    "predicted_result": "BUF",
                    "home_team_image_url": "https://example.com/buf.png",
                    "away_team_image_url": "https://example.com/nyj.png",
                    "home_coach": "Sean McDermott",
                    "away_coach": "Robert Saleh",
                    "stadium": "Highmark Stadium"
                }
            ]
        }
        mock_service.get_prediction_summary.return_value = mock_summary
        
        response = client.get("/api/fetch-data/predictions/summary?year=2020&week=1")
        
        assert response.status_code == 200
        data = response.json()
        assert data["total_games"] == 2
        assert data["predicted_home_wins"] == 2
        assert data["predicted_away_wins"] == 0

    def test_get_available_data_success(self, client, mock_service):
        """Test successful get_available_data endpoint"""
        mock_service.get_available_years_weeks.return_value = {
            "years": [2020, 2021, 2022],
            "weeks": [1, 2, 3, 4]
        }
        
        response = client.get("/api/fetch-data/available-data")
        
        assert response.status_code == 200
        data = response.json()
        assert data["years"] == [2020, 2021, 2022]
        assert data["weeks"] == [1, 2, 3, 4]

    def test_get_available_data_server_error(self, client, mock_service):
        """Test get_available_data with server error"""
        mock_service.get_available_years_weeks.side_effect = Exception("Database error")
        
        response = client.get("/api/fetch-data/available-data")
        
        assert response.status_code == 500
        assert "Internal server error" in response.json()["detail"]

    def test_health_check_success(self, client, mock_service):
        """Test successful health_check endpoint"""
        mock_service.get_available_years_weeks.return_value = {"years": [2020], "weeks": [1]}
        
        response = client.get("/api/fetch-data/health")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "API and database are accessible" in data["message"]

    def test_health_check_service_unavailable(self, client, mock_service):
        """Test health_check with service unavailable"""
        mock_service.get_available_years_weeks.side_effect = Exception("Database connection failed")
        
        response = client.get("/api/fetch-data/health")
        
        assert response.status_code == 503
        assert "Service unavailable" in response.json()["detail"]

    def test_get_match_scores_success(self, client, mock_service):
        """Test successful get_match_scores endpoint"""
        mock_service.fetch_match_scores_by_game_id.return_value = {
            "game_id": "401220116",
            "home_score": 24,
            "away_score": 17
        }
        
        response = client.get("/api/fetch-data/match-scores/401220116")
        
        assert response.status_code == 200
        data = response.json()
        assert data["game_id"] == "401220116"
        assert data["home_score"] == 24
        assert data["away_score"] == 17

    def test_get_match_scores_not_found(self, client, mock_service):
        """Test get_match_scores with game not found"""
        mock_service.fetch_match_scores_by_game_id.side_effect = ValueError("No match scores found for game_id: 401220116")
        
        response = client.get("/api/fetch-data/match-scores/401220116")
        
        assert response.status_code == 404
        assert "No match scores found for game_id: 401220116" in response.json()["detail"]

    def test_get_match_scores_server_error(self, client, mock_service):
        """Test get_match_scores with server error"""
        mock_service.fetch_match_scores_by_game_id.side_effect = Exception("Database error")
        
        response = client.get("/api/fetch-data/match-scores/401220116")
        
        assert response.status_code == 500
        assert "Internal server error" in response.json()["detail"]

    def test_missing_query_parameters(self, client):
        """Test endpoints with missing query parameters"""
        # Test missing year parameter
        response = client.get("/api/fetch-data/predictions?week=1")
        assert response.status_code == 422  # FastAPI validation error
        
        # Test missing week parameter
        response = client.get("/api/fetch-data/predictions?year=2020")
        assert response.status_code == 422  # FastAPI validation error

    def test_invalid_query_parameter_types(self, client):
        """Test endpoints with invalid query parameter types"""
        # Test non-integer year
        response = client.get("/api/fetch-data/predictions?year=abc&week=1")
        assert response.status_code == 422  # FastAPI validation error
        
        # Test non-integer week
        response = client.get("/api/fetch-data/predictions?year=2020&week=abc")
        assert response.status_code == 422  # FastAPI validation error
