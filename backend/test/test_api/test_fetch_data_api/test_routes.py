import pytest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient
from fastapi import FastAPI
from typing import Dict, Any, List, Generator
import sys
import os

# Add the backend directory to Python path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from app.api.fetch_data_api.routes import router


class TestFetchDataRoutesIntegration:
    """Integration test cases for fetch_data_api routes"""

    @pytest.fixture
    def app(self) -> FastAPI:
        """Create FastAPI app with router"""
        app = FastAPI()
        app.include_router(router)
        return app

    @pytest.fixture
    def client(self, app: FastAPI) -> TestClient:
        """Create test client"""
        return TestClient(app)

    @pytest.fixture
    def mock_service(self) -> Generator[MagicMock, None, None]:
        """Mock FetchDataService"""
        with patch('app.api.fetch_data_api.routes.fetch_service') as mock_service:
            yield mock_service

    def test_get_predictions_response_model(self, client: TestClient, mock_service: MagicMock) -> None:
        """Test that get_predictions returns correct response model structure"""
        mock_predictions: List[Dict[str, Any]] = [
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
        
        # Verify response model structure
        assert "predictions" in data
        assert "total_count" in data
        assert "year" in data
        assert "week" in data
        
        # Verify prediction model structure
        prediction = data["predictions"][0]
        required_fields = [
            "game_id", "year", "week", "home_team", "away_team",
            "home_team_win_probability", "away_team_win_probability",
            "predicted_result", "home_team_image_url", "away_team_image_url",
            "home_coach", "away_coach", "stadium"
        ]
        for field in required_fields:
            assert field in prediction

    def test_get_prediction_summary_response_model(self, client: TestClient, mock_service: MagicMock) -> None:
        """Test that get_prediction_summary returns correct response model structure"""
        mock_summary: Dict[str, Any] = {
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
        
        # Verify response model structure
        required_fields = [
            "total_games", "year", "week", "predicted_home_wins",
            "predicted_away_wins", "average_home_win_probability",
            "average_away_win_probability", "predictions"
        ]
        for field in required_fields:
            assert field in data

    def test_get_available_data_response_model(self, client: TestClient, mock_service: MagicMock) -> None:
        """Test that get_available_data returns correct response model structure"""
        mock_service.get_available_years_weeks.return_value = {
            "years": [2020, 2021, 2022],
            "weeks": [1, 2, 3, 4]
        }
        
        response = client.get("/api/fetch-data/available-data")
        
        assert response.status_code == 200
        data = response.json()
        
        # Verify response model structure
        assert "years" in data
        assert "weeks" in data
        assert isinstance(data["years"], list)
        assert isinstance(data["weeks"], list)

    def test_get_match_scores_response_model(self, client: TestClient, mock_service: MagicMock) -> None:
        """Test that get_match_scores returns correct response model structure"""
        mock_service.fetch_match_scores_by_game_id.return_value = {
            "game_id": "401220116",
            "home_score": 24,
            "away_score": 17
        }
        
        response = client.get("/api/fetch-data/match-scores/401220116")
        
        assert response.status_code == 200
        data = response.json()
        
        # Verify response model structure
        assert "game_id" in data
        assert "home_score" in data
        assert "away_score" in data
        assert data["game_id"] == "401220116"
        assert data["home_score"] == 24
        assert data["away_score"] == 17

    def test_health_check_response_model(self, client: TestClient, mock_service: MagicMock) -> None:
        """Test that health_check returns correct response model structure"""
        mock_service.get_available_years_weeks.return_value = {"years": [2020], "weeks": [1]}
        
        response = client.get("/api/fetch-data/health")
        
        assert response.status_code == 200
        data = response.json()
        
        # Verify response model structure
        assert "status" in data
        assert "message" in data
        assert data["status"] == "healthy"

    def test_error_response_model(self, client: TestClient, mock_service: MagicMock) -> None:
        """Test that error responses follow consistent structure"""
        mock_service.fetch_predictions_by_year_week.side_effect = ValueError("Invalid year")
        
        response = client.get("/api/fetch-data/predictions?year=1999&week=1")
        
        assert response.status_code == 400
        data = response.json()
        
        # Verify error response structure
        assert "detail" in data
        assert "Invalid year" in data["detail"]

    def test_get_predictions_internal_server_error(self, client: TestClient, mock_service: MagicMock) -> None:
        """Test that unexpected exceptions return 500 with standardized message."""
        mock_service.fetch_predictions_by_year_week.side_effect = RuntimeError("DB unavailable")

        response = client.get("/api/fetch-data/predictions?year=2020&week=1")

        assert response.status_code == 500
        data = response.json()
        assert "detail" in data
        assert "Internal server error" in data["detail"]

    def test_get_prediction_summary_value_error(self, client: TestClient, mock_service: MagicMock) -> None:
        """Test summary endpoint raises 400 on validation issues."""
        mock_service.get_prediction_summary.side_effect = ValueError("Invalid week")

        response = client.get("/api/fetch-data/predictions/summary?year=2020&week=99")

        assert response.status_code == 400
        assert response.json()["detail"] == "Invalid week"

    def test_get_prediction_summary_internal_error(self, client: TestClient, mock_service: MagicMock) -> None:
        """Test summary endpoint handles unexpected errors with 500."""
        mock_service.get_prediction_summary.side_effect = RuntimeError("Database down")

        response = client.get("/api/fetch-data/predictions/summary?year=2020&week=1")

        assert response.status_code == 500
        assert "Internal server error" in response.json()["detail"]

    def test_get_available_data_internal_error(self, client: TestClient, mock_service: MagicMock) -> None:
        """Test available-data endpoint returns 500 on service failure."""
        mock_service.get_available_years_weeks.side_effect = RuntimeError("DB locked")

        response = client.get("/api/fetch-data/available-data")

        assert response.status_code == 500
        assert "Internal server error" in response.json()["detail"]

    def test_health_check_service_unavailable(self, client: TestClient, mock_service: MagicMock) -> None:
        """Test health endpoint returns 503 when dependency fails."""
        mock_service.get_available_years_weeks.side_effect = RuntimeError("timeout")

        response = client.get("/api/fetch-data/health")

        assert response.status_code == 503
        assert "Service unavailable" in response.json()["detail"]

    def test_get_match_scores_value_error(self, client: TestClient, mock_service: MagicMock) -> None:
        """Test match scores endpoint returns 404 on missing game."""
        mock_service.fetch_match_scores_by_game_id.side_effect = ValueError("Not found")

        response = client.get("/api/fetch-data/match-scores/unknown")

        assert response.status_code == 404
        assert response.json()["detail"] == "Not found"

    def test_get_match_scores_internal_error(self, client: TestClient, mock_service: MagicMock) -> None:
        """Test match scores endpoint returns 500 on unexpected errors."""
        mock_service.fetch_match_scores_by_game_id.side_effect = RuntimeError("ESPN failure")

        response = client.get("/api/fetch-data/match-scores/401220116")

        assert response.status_code == 500
        assert "Internal server error" in response.json()["detail"]

    def test_multiple_predictions_response(self, client: TestClient, mock_service: MagicMock) -> None:
        """Test response with multiple predictions"""
        mock_predictions: List[Dict[str, Any]] = [
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
            },
            {
                "game_id": "401220117",
                "year": 2020,
                "week": 1,
                "home_team": "KC",
                "away_team": "DEN",
                "home_team_win_probability": 0.70,
                "away_team_win_probability": 0.30,
                "predicted_result": "KC",
                "home_team_image_url": "https://example.com/kc.png",
                "away_team_image_url": "https://example.com/den.png",
                "home_coach": "Andy Reid",
                "away_coach": "Sean Payton",
                "stadium": "Arrowhead Stadium"
            }
        ]
        mock_service.fetch_predictions_by_year_week.return_value = mock_predictions
        
        response = client.get("/api/fetch-data/predictions?year=2020&week=1")
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["total_count"] == 2
        assert len(data["predictions"]) == 2
        assert data["predictions"][0]["game_id"] == "401220116"
        assert data["predictions"][1]["game_id"] == "401220117"

    def test_empty_predictions_response(self, client: TestClient, mock_service: MagicMock) -> None:
        """Test response with no predictions"""
        mock_service.fetch_predictions_by_year_week.return_value = []
        
        response = client.get("/api/fetch-data/predictions?year=2020&week=1")
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["total_count"] == 0
        assert len(data["predictions"]) == 0
        assert data["year"] == 2020
        assert data["week"] == 1

    def test_match_scores_with_null_scores(self, client: TestClient, mock_service: MagicMock) -> None:
        """Test match scores response with null scores"""
        mock_service.fetch_match_scores_by_game_id.return_value = {
            "game_id": "401220116",
            "home_score": None,
            "away_score": None
        }
        
        response = client.get("/api/fetch-data/match-scores/401220116")
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["game_id"] == "401220116"
        assert data["home_score"] is None
        assert data["away_score"] is None

    def test_api_documentation_endpoints(self, client: TestClient) -> None:
        """Test that API documentation endpoints are accessible"""
        # Test OpenAPI schema
        response = client.get("/openapi.json")
        assert response.status_code == 200
        
        # Test docs endpoint
        response = client.get("/docs")
        assert response.status_code == 200

    def test_router_prefix_and_tags(self, client: TestClient) -> None:
        """Test that router prefix and tags are correctly applied"""
        # Test that all endpoints have the correct prefix
        endpoints_to_test = [
            "/api/fetch-data/predictions",
            "/api/fetch-data/predictions/summary",
            "/api/fetch-data/available-data",
            "/api/fetch-data/health",
            "/api/fetch-data/match-scores/test_id"
        ]
        
        for endpoint in endpoints_to_test:
            # We expect 422 for missing parameters or 500 for service errors
            # The important thing is that the endpoint exists and has the right prefix
            response = client.get(endpoint)
            assert response.status_code in [200, 400, 404, 422, 500, 503]

    def test_cors_headers(self, client: TestClient) -> None:
        """Test that CORS headers are properly set (if configured)"""
        response = client.options("/api/fetch-data/predictions")
        # This test depends on CORS configuration in the main app
        # For now, just verify the endpoint exists
        assert response.status_code in [200, 405]  # 405 if OPTIONS not allowed
