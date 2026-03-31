import sys
import os
import sqlite3
from typing import List, Dict, Any
from dotenv import load_dotenv

# Add the backend directory to Python path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from app.pipeline.crud_db import fetch_predictions, fetch_match_scores, fetch_prediction_by_game_id
from app.core.query import fetch_data_query, fetch_match_scores_query, fetch_prediction_by_game_id_query

_ = load_dotenv()

class FetchDataService:
    """
    Service class for handling fetch predictions business logic
    """
    
    def __init__(self) -> None:
        """Initialize the service with database connection"""
        super().__init__()
        self.database_path = os.getenv("database_path")
        if not self.database_path:
            # Default database path relative to the backend directory
            backend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
            self.database_path = os.path.join(backend_dir, "data", "database", "nfl_predictions.db")
    
    def get_database_connection(self) -> sqlite3.Connection:
        """
        Create and return a database connection
        
        Returns:
            sqlite3.Connection: Database connection object
        """
        try:
            conn = sqlite3.connect(self.database_path)  # type: ignore
            return conn
        except sqlite3.Error as e:
            raise sqlite3.DatabaseError(f"Database connection failed: {str(e)}")
    
    def fetch_predictions_by_year_week(self, year: int, week: int) -> List[Dict[str, Any]]:
        """
        Fetch prediction data for a specific year and week
        
        Args:
            year (int): The NFL season year
            week (int): The week number in the season
            
        Returns:
            List[Dict[str, Any]]: List of prediction records as dictionaries
            
        Raises:
            ValueError: If year or week parameters are invalid
            Exception: If database operation fails
        """
        # Validate input parameters
        if year < 2000 or year > 2030:
            raise ValueError("Year must be an integer between 2000 and 2030")
        
        if week < 1 or week > 53:
            raise ValueError("Week must be an integer between 1 and 53")
        
        conn = None
        try:
            conn = self.get_database_connection()
            cursor = conn.cursor()
            
            # Fetch predictions using the existing function
            raw_data = fetch_predictions(cursor, fetch_data_query, year, week)
            
            # Convert raw tuples to dictionaries for better API response
            predictions: List[Dict[str, Any]] = []
            for record in raw_data:
                prediction_dict: Dict[str, Any] = {
                    "game_id": str(record[0]),  # Convert to string
                    "year": record[1],
                    "week": record[2],
                    "home_team": record[3],
                    "away_team": record[4],
                    "home_team_win_probability": record[5],
                    "away_team_win_probability": record[6],
                    "predicted_result": record[7],
                    "home_team_image_url": record[8],
                    "away_team_image_url": record[9],
                    "home_coach": record[10],
                    "away_coach": record[11],
                    "stadium": record[12],
                    "home_score": record[13] if len(record) > 13 else None,
                    "away_score": record[14] if len(record) > 14 else None
                }
                predictions.append(prediction_dict)
            
            return predictions
            
        except sqlite3.Error as e:
            raise sqlite3.DatabaseError(f"Database query failed: {str(e)}")
        except Exception as e:
            raise RuntimeError(f"Failed to fetch predictions: {str(e)}")
        finally:
            if conn:
                conn.close()
    
    def get_available_years_weeks(self) -> Dict[str, List[int]]:
        """
        Get all available years and weeks from the database
        
        Returns:
            Dict[str, List[int]]: Dictionary with 'years' and 'weeks' keys
        """
        conn = None
        try:
            conn = self.get_database_connection()
            cursor = conn.cursor()
            
            # Get distinct years
            _ = cursor.execute("SELECT DISTINCT year FROM match_predictions ORDER BY year")
            years = [row[0] for row in cursor.fetchall()]
            
            # Get distinct weeks
            _ = cursor.execute("SELECT DISTINCT week FROM match_predictions ORDER BY week")
            weeks = [row[0] for row in cursor.fetchall()]
            
            return {
                "years": years,
                "weeks": weeks
            }
            
        except sqlite3.Error as e:
            raise sqlite3.DatabaseError(f"Database query failed: {str(e)}")
        except Exception as e:
            raise RuntimeError(f"Failed to get available years and weeks: {str(e)}")
        finally:
            if conn:
                conn.close()
    
    def get_prediction_summary(self, year: int, week: int) -> Dict[str, Any]:
        """
        Get a summary of predictions for a specific year and week
        
        Args:
            year (int): The NFL season year
            week (int): The week number in the season
            
        Returns:
            Dict[str, Any]: Summary statistics
        """
        predictions = self.fetch_predictions_by_year_week(year, week)
        
        if not predictions:
            return {
                "total_games": 0,
                "year": year,
                "week": week,
                "predicted_home_wins": 0,
                "predicted_away_wins": 0,
                "average_home_win_probability": 0.0,
                "average_away_win_probability": 0.0,
                "predictions": []
            }
        
        # Calculate summary statistics
        total_games = len(predictions)
        home_wins = sum(1 for p in predictions if p["predicted_result"] == p["home_team"])
        away_wins = total_games - home_wins
        
        avg_home_prob = sum(p["home_team_win_probability"] for p in predictions) / total_games
        avg_away_prob = sum(p["away_team_win_probability"] for p in predictions) / total_games
        
        return {
            "total_games": total_games,
            "year": year,
            "week": week,
            "predicted_home_wins": home_wins,
            "predicted_away_wins": away_wins,
            "average_home_win_probability": round(avg_home_prob, 3),
            "average_away_win_probability": round(avg_away_prob, 3),
            "predictions": predictions
        }
    
    def fetch_prediction_by_game_id(self, game_id: str) -> Dict[str, Any]:
        """
        Fetch a complete prediction record for a specific game by game_id
        
        Args:
            game_id (str): Unique identifier for the NFL game
            
        Returns:
            Dict[str, Any]: Complete prediction data including all game details
            
        Raises:
            ValueError: If game_id parameter is invalid or not found
            Exception: If database operation fails
        """
        # Validate input parameters
        if not game_id or not str(game_id).strip():
            raise ValueError("Game ID must be a non-empty string")
        
        conn = None
        try:
            conn = self.get_database_connection()
            cursor = conn.cursor()
            
            # Fetch prediction using the existing function
            raw_data = fetch_prediction_by_game_id(cursor, fetch_prediction_by_game_id_query, str(game_id))
            
            if not raw_data:
                raise ValueError(f"No prediction found for game_id: {game_id}")
            
            # Convert raw tuple to dictionary
            record = raw_data[0]  # fetch_prediction_by_game_id returns list of tuples
            prediction_dict: Dict[str, Any] = {
                "game_id": str(record[0]),
                "year": record[1],
                "week": record[2],
                "home_team": record[3],
                "away_team": record[4],
                "home_team_win_probability": record[5],
                "away_team_win_probability": record[6],
                "predicted_result": record[7],
                "home_team_image_url": record[8],
                "away_team_image_url": record[9],
                "home_coach": record[10],
                "away_coach": record[11],
                "stadium": record[12],
                "home_score": record[13] if len(record) > 13 else None,
                "away_score": record[14] if len(record) > 14 else None
            }
            
            return prediction_dict
            
        except sqlite3.Error as e:
            raise sqlite3.DatabaseError(f"Database query failed: {str(e)}")
        except Exception as e:
            raise RuntimeError(f"Failed to fetch prediction: {str(e)}")
        finally:
            if conn:
                conn.close()
    
    def fetch_match_scores_by_game_id(self, game_id: str) -> Dict[str, Any]:
        """
        Fetch match scores for a specific game
        
        Args:
            game_id (str): Unique identifier for the NFL game
            
        Returns:
            Dict[str, Any]: Match score data including game_id, home_score, and away_score
            
        Raises:
            ValueError: If game_id parameter is invalid
            Exception: If database operation fails
        """
        # Validate input parameters
        if not game_id.strip():
            raise ValueError("Game ID must be a non-empty string")
        
        conn = None
        try:
            conn = self.get_database_connection()
            cursor = conn.cursor()
            
            # Fetch match scores using the existing function
            raw_data = fetch_match_scores(cursor, fetch_match_scores_query, game_id)
            
            if not raw_data:
                raise ValueError(f"No match scores found for game_id: {game_id}")
            
            # Convert raw tuple to dictionary
            record = raw_data[0]  # fetch_match_scores returns list of tuples
            match_score_dict: Dict[str, Any] = {
                "game_id": str(record[0]),
                "home_score": record[1],
                "away_score": record[2]
            }
            
            return match_score_dict
            
        except sqlite3.Error as e:
            raise sqlite3.DatabaseError(f"Database query failed: {str(e)}")
        except Exception as e:
            raise RuntimeError(f"Failed to fetch match scores: {str(e)}")
        finally:
            if conn:
                conn.close()
