from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from pydantic import BaseModel
import sys
import os

# Add the backend directory to Python path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from .services import FetchDataService

# Create router instance
router = APIRouter(prefix="/api/fetch-data", tags=["fetch-data"])

# Initialize service
fetch_service = FetchDataService()

# Pydantic models for request/response
class PredictionResponse(BaseModel):
    """Response model for individual prediction"""
    game_id: str
    year: int
    week: int
    home_team: str
    away_team: str
    home_team_win_probability: float
    away_team_win_probability: float
    predicted_result: str
    home_team_image_url: str
    away_team_image_url: str
    home_coach: str
    away_coach: str
    stadium: str
    home_score: Optional[int] = None
    away_score: Optional[int] = None

class PredictionsListResponse(BaseModel):
    """Response model for list of predictions"""
    predictions: List[PredictionResponse]
    total_count: int
    year: int
    week: int

class SummaryResponse(BaseModel):
    """Response model for prediction summary"""
    total_games: int
    year: int
    week: int
    predicted_home_wins: int
    predicted_away_wins: int
    average_home_win_probability: float
    average_away_win_probability: float
    predictions: List[PredictionResponse]

class AvailableDataResponse(BaseModel):
    """Response model for available years and weeks"""
    years: List[int]
    weeks: List[int]

class MatchScoreResponse(BaseModel):
    """Response model for match scores"""
    game_id: str
    home_score: Optional[int] = None
    away_score: Optional[int] = None

class ErrorResponse(BaseModel):
    """Error response model"""
    error: str
    detail: Optional[str] = None

@router.get(
    "/predictions",
    response_model=PredictionsListResponse,
    summary="Fetch NFL game predictions",
    description="Retrieve NFL game predictions for a specific year and week"
)
async def get_predictions(
    year: int = Query(..., description="NFL season year", example=2020),  # type: ignore
    week: int = Query(..., description="Week number in the season", example=1)  # type: ignore
):
    """
    Fetch NFL game predictions for a specific year and week
    
    Args:
        year: The NFL season year (e.g., 2020)
        week: The week number in the season (1-53)
    
    Returns:
        List of predictions with game details and win probabilities
    """
    try:
        predictions = fetch_service.fetch_predictions_by_year_week(year, week)
        
        # Convert to response model
        prediction_responses = [
            PredictionResponse(**prediction) for prediction in predictions
        ]
        
        return PredictionsListResponse(
            predictions=prediction_responses,
            total_count=len(prediction_responses),
            year=year,
            week=week
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.get(
    "/predictions/summary",
    response_model=SummaryResponse,
    summary="Get prediction summary",
    description="Get summary statistics for predictions in a specific year and week"
)
async def get_prediction_summary(
    year: int = Query(..., description="NFL season year", example=2020),  # type: ignore
    week: int = Query(..., description="Week number in the season", example=1)  # type: ignore
):
    """
    Get summary statistics for predictions in a specific year and week
    
    Args:
        year: The NFL season year (e.g., 2020)
        week: The week number in the season (1-53)
    
    Returns:
        Summary statistics including total games, win counts, and average probabilities
    """
    try:
        summary = fetch_service.get_prediction_summary(year, week)
        
        # Convert predictions to response model
        prediction_responses = [
            PredictionResponse(**prediction) for prediction in summary["predictions"]
        ]
        
        return SummaryResponse(
            total_games=summary["total_games"],
            year=summary["year"],
            week=summary["week"],
            predicted_home_wins=summary["predicted_home_wins"],
            predicted_away_wins=summary["predicted_away_wins"],
            average_home_win_probability=summary["average_home_win_probability"],
            average_away_win_probability=summary["average_away_win_probability"],
            predictions=prediction_responses
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.get(
    "/predictions/{game_id}",
    response_model=PredictionResponse,
    summary="Fetch prediction by game ID",
    description="Retrieve a complete prediction record for a specific game by game_id"
)
async def get_prediction_by_game_id(game_id: str):
    """
    Fetch a complete prediction record for a specific game by game_id
    
    Args:
        game_id: Unique identifier for the NFL game
    
    Returns:
        Complete prediction data including all game details
    """
    try:
        prediction = fetch_service.fetch_prediction_by_game_id(game_id)
        return PredictionResponse(**prediction)
        
    except ValueError as e:
        # ValueError can mean invalid game_id or not found
        error_message = str(e)
        if "No prediction found" in error_message:
            raise HTTPException(status_code=404, detail=error_message)
        raise HTTPException(status_code=400, detail=error_message)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.get(
    "/available-data",
    response_model=AvailableDataResponse,
    summary="Get available years and weeks",
    description="Get all available years and weeks in the database"
)
async def get_available_data():
    """
    Get all available years and weeks in the database
    
    Returns:
        Dictionary containing lists of available years and weeks
    """
    try:
        available_data = fetch_service.get_available_years_weeks()
        return AvailableDataResponse(**available_data)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.get(
    "/health",
    summary="Health check",
    description="Check if the API is running and database is accessible"
)
async def health_check():
    """
    Health check endpoint to verify API and database connectivity
    
    Returns:
        Status message
    """
    try:
        # Try to get available data to test database connection
        _ = fetch_service.get_available_years_weeks()
        return {"status": "healthy", "message": "API and database are accessible"}
        
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Service unavailable: {str(e)}")

@router.get(
    "/match-scores/{game_id}",
    response_model=MatchScoreResponse,
    summary="Fetch match scores by game ID",
    description="Retrieve the actual scores for a specific NFL game"
)
async def get_match_scores(game_id: str):
    """
    Fetch match scores for a specific game by game ID
    
    Args:
        game_id: Unique identifier for the NFL game
    
    Returns:
        Match score data including home_score and away_score
    """
    try:
        match_scores = fetch_service.fetch_match_scores_by_game_id(game_id)
        return MatchScoreResponse(**match_scores)
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
