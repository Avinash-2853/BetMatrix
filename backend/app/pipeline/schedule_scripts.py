import time
import pandas as pd
import numpy as np
import sqlite3
import pickle
import os
from typing import Dict, Any, Optional, Tuple, Union
import requests
from dotenv import load_dotenv
from app.pipeline.get_data import map_team, get_coach_name
from app.pipeline.clean import aggregate_match_features_with_nulls, aggregate_categorical_counts, aggregate_positive_negative, remove_columns
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
from app.pipeline.glicko import add_glicko_features
from app.pipeline.data_load import load_nfl_data
from app.pipeline.get_data import get_team_details, get_match_scores, get_game_date
from app.pipeline.inference import get_inputs
from app.pipeline.crud_db import insert_prediction_data, update_actual_result, fetch_predictions, fetch_match_scores as fetch_scores_from_db, fetch_all_predictions, update_probabilities
from app.core.query import insert_prediction_data_query, update_actual_result_query, fetch_data_query, fetch_match_scores_query, fetch_all_predictions_query, update_probabilities_query
from app.core.constant import (
    DATA_LOAD_STARTED, DATA_CLEANING_STARTED, AGGREGATE_MATCH_FEATURES_STARTED,
    AGGREGATE_CATEGORICAL_FEATURES_STARTED, AGGREGATE_POSITIVE_NEGATIVE_STARTED,
    FILTER_GAME_STATS_STARTED, REMOVE_COLUMNS_STARTED, NORMALIZE_YARDS_FEATURES_STARTED,
    ADD_PREVIOUS_MATCH_RESULTS_STARTED, REMOVE_DRAWS_STARTED, ADD_LAST_5_MATCHES_STATS_STARTED,
    ADD_GLICKO_FEATURES_STARTED, ADD_PREVIOUS_FEATURES_STARTED, ADD_LAST_5_H2H_WIN_RATIO_STARTED,
    DATA_PREPROCESSING_STARTED, LABEL_ENCODING_STARTED, SCALING_STARTED, SMOTE_STARTED,
    MODEL_TRAINING_STARTED, LOADING_MODEL_AND_ENCODERS, MODEL_AND_ENCODERS_LOADED,
    CONNECTED_TO_DATABASE, UNKNOWN_FEATURE_ENCODING, SKIPPING_GAME_MISSING_DATA,
    PROCESSING_GAME, PREDICTION_STORED, PROCESSING_WEEK, NO_GAMES_FOUND, FOUND_GAMES_FOR_WEEK,
    ERROR_PROCESSING_GAME, WEEK_COMPLETED, STARTING_WEEKLY_PREDICTIONS, MODEL_OR_ENCODER_NOT_FOUND,
    ERROR_LOADING_MODEL_ENCODERS, DATABASE_CONNECTION_FAILED, ERROR_PROCESSING_WEEK,
    COMPLETED_PREDICTIONS, STARTING_MATCH_RESULTS_UPDATE, CHECKING_WEEK, NO_PREDICTIONS_FOUND,
    FOUND_PREDICTIONS_FOR_WEEK, GAME_ALREADY_HAS_SCORES, CHECKING_GAME_DATE,
    COULD_NOT_RETRIEVE_GAME_DATE, GAME_SCHEDULED_FUTURE, COULD_NOT_PARSE_GAME_DATE,
    FETCHING_SCORES_FOR_GAME, MATCH_SCORES_UNAVAILABLE, INCOMPLETE_SCORES, INVALID_SCORE_TYPES,
    UPDATED_GAME, ERROR_PROCESSING_GAME_UPDATE, WEEK_PROCESSED, ERROR_PROCESSING_WEEK_UPDATE,
    MATCH_RESULTS_UPDATE_COMPLETED, STARTING_FUTURE_PREDICTIONS_UPDATE, FETCHING_ALL_PREDICTIONS,
    GAME_PAST_OR_TODAY, UPDATING_PREDICTION_FOR_GAME, UPDATED_PREDICTION_FOR_GAME, COULD_NOT_PARSE_GAME_DATE_FUTURE,
    CALCULATING_PREDICTION_ACCURACY, PREDICTION_ACCURACY_STARTED, PREDICTION_ACCURACY_WEEK,
    PREDICTION_ACCURACY_SUMMARY, PREDICTION_ACCURACY_NO_DATA, PREDICTION_ACCURACY_COMPLETED,
    ERROR_PROCESSING_GAME_FUTURE, FUTURE_PREDICTIONS_UPDATE_COMPLETED,
    COULD_NOT_RETRIEVE_GAME_DATE_FUTURE, FOUND_PREDICTIONS_IN_DATABASE,
    ADD_HISTORICAL_WIN_PCT_STARTED, ADD_PF_PA_BY_SEASON_STARTED,
    ADD_HOME_AWAY_TEAM_AVG_SCORES_BEFORE_STARTED, ADD_LEAGUE_AVG_SCORE_BEFORE_STARTED,
    ADD_HOME_AWAY_TEAM_AVG_STAT_BEFORE_STARTED, ADD_LEAGUE_AVG_STAT_BEFORE_STARTED
)
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from imblearn.over_sampling import SMOTEN
from catboost import CatBoostClassifier, Pool
import logging
logger = logging.getLogger(__name__)

# Load environment variables
_ = load_dotenv()

# Get paths from environment or use defaults
current_dir = os.path.dirname(os.path.abspath(__file__))
backend_root = os.path.abspath(os.path.join(current_dir, "../.."))

# Model paths
models_path = os.getenv("models_path")
if not models_path:
    models_path = os.path.join(backend_root, "models")
else:
    if not os.path.isabs(models_path):
        models_path = os.path.join(backend_root, models_path)

# Database path
database_path = os.getenv("database_path")
if not database_path:
    database_path = os.path.join(backend_root, "data", "database", "nfl_predictions.db")
else:
    if not os.path.isabs(database_path):
        database_path = os.path.join(backend_root, database_path)

# Input CSV path for get_inputs - use step3_nfl_processed_data.csv only (no fallback, no env override)
input_csv_path = os.path.join(backend_root, "data", "input", "step3_nfl_processed_data.csv")

def data_load():
    """
    Load NFL play-by-play data for all seasons from 1999 to current year.
    
    This function retrieves historical NFL data using the nflreadpy library.
    The data includes play-by-play statistics for all games from 1999 onwards.
    
    Returns:
        pd.DataFrame: DataFrame containing NFL play-by-play data with columns
            including game_id, play_id, season, week, teams, scores, and various
            game statistics.
            
    Example:
        >>> df = data_load()
        >>> print(df.shape)
        (1000000, 50)  # Example output
    """
    logger.info(DATA_LOAD_STARTED)
    # Get current year to determine the range of seasons to load
    # Get current date to determine the range of seasons to load
    current_time = time.localtime()
    current_year = current_time.tm_year
    
    # NFL season starts in September. If we are in Jan-Aug, the "current" season is the previous year.
    # E.g., in Jan 2026, we are finishing the 2025 season.
    if current_time.tm_mon < 9:
        end_year = current_year - 1
    else:
        end_year = current_year
        
    # Create list of all seasons from 1999 to end_year (inclusive)
    year_list = list(range(1999, end_year + 1))
    # Load NFL data for all specified seasons
    df = load_nfl_data(seasons=year_list)
    return df

def data_cleaning(df):
    """
    Clean and preprocess NFL play-by-play data by aggregating features and removing irrelevant columns.
    
    This function performs several cleaning operations:
    1. Aggregates match-level features from play-by-play data
    2. Handles categorical and numerical features separately
    3. Filters for end-of-game records (game_seconds_remaining == 0)
    4. Removes redundant columns
    5. Normalizes yard-based features
    
    Args:
        df (pd.DataFrame): Raw NFL play-by-play DataFrame from data_load()
        
    Returns:
        pd.DataFrame: Cleaned DataFrame with aggregated features, normalized values,
            and only relevant columns retained. Contains match-level statistics
            rather than play-level data.
            
    Note:
        - Only end-of-game records are kept (final game states)
        - Yard features are normalized by total yards gained
        - Field goal and extra point result columns are removed after aggregation
    """
    logger.info(DATA_CLEANING_STARTED)
    # List of numerical features to aggregate at match level
    features = ["first_down_rush", "first_down_pass",
                "third_down_converted", 
                "fourth_down_converted", "interception", 
                "fumble_lost","fumble_forced", "rush_attempt", 
                "pass_attempt", "pass_touchdown", 
                "qb_dropback","rush_touchdown","tackled_for_loss",
                "qb_hit", "punt_attempt","kickoff_attempt",
                    "kickoff_inside_twenty", "penalty_yards", "rushing_yards",
                    "passing_yards", "receiving_yards","yards_gained","sack"]
    # Categorical features to aggregate as counts
    cat_features = ["field_goal_result", "extra_point_result"]
    # Numerical features that can be positive or negative (need special handling)
    num_features = ["return_yards", "yards_after_catch"]

    logger.info(AGGREGATE_MATCH_FEATURES_STARTED)
    df = aggregate_match_features_with_nulls(df, stat_cols=features)
    logger.info(AGGREGATE_CATEGORICAL_FEATURES_STARTED)
    df = aggregate_categorical_counts(df, cat_cols=cat_features)
    logger.info(AGGREGATE_POSITIVE_NEGATIVE_STARTED)
    df = aggregate_positive_negative(df, num_cols=num_features)
    logger.info(FILTER_GAME_STATS_STARTED)
    df = df[df["game_seconds_remaining"] == 0]

    logger.info(REMOVE_COLUMNS_STARTED)
    extra_list = ["play_id","week",
                  "season_type", "stadium",
                  "roof","surface","location",
                  "temp", "wind", "home_score",
                  "away_score", "posteam", "posteam_score",
                  "defteam_score", "score_differential",
                  "fumble_recovery_1_yards", "field_goal_attempt",
                  "extra_point_attempt","spread_line","total_line",
                  "vegas_wp", "total_home_comp_air_epa",
                  "total_away_comp_air_epa","total_home_comp_yac_epa",
                  "total_away_comp_yac_epa","total_home_raw_air_epa",
                  "total_home_raw_air_epa", "total_home_raw_yac_epa",
                  "total_home_comp_air_wpa", "total_away_comp_air_wpa",
                  "total_home_comp_yac_wpa","total_away_comp_yac_wpa",
                  "total_home_raw_air_wpa", "total_away_raw_air_wpa",
                  "total_home_raw_yac_wpa", "total_away_raw_yac_wpa",
                  "quarter_seconds_remaining","half_seconds_remaining",
                  "game_seconds_remaining", "total_away_raw_air_epa"]

    remove_list = features + num_features + cat_features + extra_list
    df = remove_columns(df, column_list=remove_list)

    logger.info(NORMALIZE_YARDS_FEATURES_STARTED)
    # List of yard-based features to normalize by total yards gained
    yard_list = ["home_penalty_yards", "home_rushing_yards",
                 "home_passing_yards","home_receiving_yards",
                 "home_yards_gained","away_penalty_yards",
                 "away_rushing_yards","away_passing_yards",
                 "away_receiving_yards","away_yards_gained"]

    # Normalize yard features by dividing by total yards gained
    # This creates percentage/proportion features that are scale-invariant
    for i in yard_list:
        if "home" in i:
            # Normalize home team yard features by home total yards
            df[i] = df[i] / df["home_yards_gained"]

        if "away" in i:
            # Normalize away team yard features by away total yards
            df[i] = df[i] / df["away_yards_gained"]

    remove_list = ["home_field_goal_result_blocked","home_field_goal_result_made",
                    "home_field_goal_result_missed","away_field_goal_result_made",
                    "away_field_goal_result_blocked","away_field_goal_result_missed",
                    "home_extra_point_result_aborted","home_extra_point_result_blocked",
                    "home_extra_point_result_failed","home_extra_point_result_good",
                    "away_extra_point_result_aborted","away_extra_point_result_blocked",
                    "away_extra_point_result_failed","away_extra_point_result_good"]
    df = remove_columns(df, column_list=remove_list)
    return df

def feature_engineering(df):
    """
    Perform feature engineering on cleaned NFL data.
    
    This function creates advanced features including:
    1. Match result labels (win/loss for home team)
    2. Last 5 games statistics for each team
    3. Glicko rating system features (team strength ratings)
    4. Previous game performance features
    5. Head-to-head win ratios
    
    Args:
        df (pd.DataFrame): Cleaned DataFrame from data_cleaning()
        
    Returns:
        pd.DataFrame: DataFrame with engineered features added. Contains:
            - match_result: binary outcome (0=home loss, 1=home win)
            - last_5_* features: rolling statistics from last 5 games
            - glicko_rating features: team strength ratings
            - prev_* features: previous game performance metrics
            - last_5_h2h_win_ratio: head-to-head win ratio
            
    Note:
        - Draws (match_result == 0.5) are removed as they're not useful for binary classification
        - All features are computed per team and per match
    """
    # Add previous match results to identify wins/losses
    logger.info(ADD_PREVIOUS_MATCH_RESULTS_STARTED)
    df  = add_match_result(df)
    
    logger.info(ADD_HISTORICAL_WIN_PCT_STARTED)
    # Calculate historical win percentage for home and away teams
    df = add_historical_win_pct(df)
    
    logger.info(ADD_PF_PA_BY_SEASON_STARTED)
    # Add Points For (PF) and Points Against (PA) stats by season
    df = add_pf_pa_by_season(df)
    
    logger.info(ADD_HOME_AWAY_TEAM_AVG_SCORES_BEFORE_STARTED)
    # Add average scores for home and away teams before the current match
    df = add_home_away_team_avg_scores_before(df)
    
    logger.info(ADD_LEAGUE_AVG_SCORE_BEFORE_STARTED)
    # Add league average scores before the current match for comparison
    df = add_league_avg_score_before(df)
    eps = 1e-6

    # Compute net rating features (log-transformed, with epsilon for numerical stability)
    df["home_net_rating"] = np.log((df["home_pf"] + eps) / (df["home_pa"] + eps))
    df["away_net_rating"] = np.log((df["away_pf"] + eps) / (df["away_pa"] + eps))
    # Defensive efficiency: lower is better (log-transformed)
    df["home_defense"] = np.log((df["home_pa"] + eps) / (df["home_pf"] + eps))
    df["away_defense"] = np.log((df["away_pa"] + eps) / (df["away_pf"] + eps))

    # Offensive efficiency for symmetry with defense ("offense" = PF/PA, log-transformed)
    df["home_offense"] = np.log((df["home_pf"] + eps) / (df["home_pa"] + eps))
    df["away_offense"] = np.log((df["away_pf"] + eps) / (df["away_pa"] + eps))

    df["home_team_peformance"] = np.log(
    (df["home_team_avg_score"] + 1e-6) / (df["league_avg_score_before"] + 1e-6))
    df["away_team_peformance"] = np.log(
        (df["away_team_avg_score"] + 1e-6) / (df["league_avg_score_before"] + 1e-6)
    )

    
    logger.info(ADD_HOME_AWAY_TEAM_AVG_STAT_BEFORE_STARTED)
    # Add average passing yards for home and away teams
    df = add_home_away_team_avg_stat_before(
    df,
    stat_name="passing_yards",
    home_stat_col="home_passing_yards",
    away_stat_col="away_passing_yards")


    df = add_home_away_team_avg_stat_before(
        df,
        stat_name="receiving_yards",
        home_stat_col="home_receiving_yards",
        away_stat_col="away_receiving_yards"
    )

    df = add_home_away_team_avg_stat_before(
        df,
        stat_name="rushing_yards",
        home_stat_col="home_rushing_yards",
        away_stat_col="away_rushing_yards"
    )

    df = add_home_away_team_avg_stat_before(
        df,
        stat_name="yards_gained",
        home_stat_col="home_yards_gained",
        away_stat_col="away_yards_gained"
    )



    
    logger.info(ADD_LEAGUE_AVG_STAT_BEFORE_STARTED)
    # Add league average passing yards
    df = add_league_avg_stat_before(
        df,
        stat_name="passing_yards",
        home_stat_col="home_passing_yards",
        away_stat_col="away_passing_yards"
    )


    df = add_league_avg_stat_before(
        df,
        stat_name="receiving_yards",
        home_stat_col="home_receiving_yards",
        away_stat_col="away_receiving_yards"
    )



    df = add_league_avg_stat_before(
        df,
        stat_name="rushing_yards",
        home_stat_col="home_rushing_yards",
        away_stat_col="away_rushing_yards"
    )

    df = add_league_avg_stat_before(
        df, 
        stat_name="yards_gained",
        home_stat_col="home_yards_gained",
        away_stat_col="away_yards_gained"

    )

    df["home_avg_passing_yards"] = np.log((df["home_avg_passing_yards"] + 1e-6)/ (df["league_avg_passing_yards_before"] + 1e-6))
    df["home_avg_receiving_yards"] = np.log((df["home_avg_receiving_yards"] + 1e-6 ) / (df["league_avg_receiving_yards_before"] + 1e-6))
    df["home_avg_rushing_yards"] = np.log((df["home_avg_rushing_yards"] + 1e-6) / (df["league_avg_rushing_yards_before"] + 1e-6))
    df["home_avg_yards_gained"] = np.log((df["home_avg_yards_gained"] + 1e-6) / (df["league_avg_yards_gained_before"] + 1e-6))


    # Remove draws (match_result == 0.5) as they're not useful for binary classification
    logger.info(REMOVE_DRAWS_STARTED)
    

    logger.info(ADD_LAST_5_MATCHES_STATS_STARTED)
    temp_df = pd.DataFrame({
        "home" : ['total_home_score', 'total_home_epa', 
                  'total_home_rush_epa', 'total_home_pass_epa', 
                  'home_first_down_rush', 'home_first_down_pass', 
                  'home_third_down_converted', 'home_fourth_down_converted', 
                  'home_interception', 'home_fumble_lost', 'home_fumble_forced', 
                  'home_rush_attempt', 'home_pass_attempt', 'home_pass_touchdown', 
                  'home_qb_dropback', 'home_rush_touchdown', 'home_tackled_for_loss', 
                  'home_qb_hit', 'home_punt_attempt', 'home_kickoff_attempt', 
                  'home_kickoff_inside_twenty', 'home_penalty_yards', 
                  'home_rushing_yards', 'home_passing_yards', 'home_receiving_yards', 
                  'home_yards_gained', 'home_sack', 'home_return_yards_positive', 
                  'home_return_yards_negative', 'home_yards_after_catch_positive', 
                  'home_yards_after_catch_negative', 'home_pct','home_pf','home_pa',
                  'home_team_avg_score','home_net_rating','home_defense','home_offense',
                  'home_team_peformance','home_avg_passing_yards','home_avg_receiving_yards',
                  'home_avg_rushing_yards','home_avg_yards_gained'],
        "away" : ['total_away_score', 'total_away_epa', 'total_away_rush_epa', 
                  'total_away_pass_epa', 'away_first_down_rush', 'away_first_down_pass', 
                  'away_third_down_converted', 'away_fourth_down_converted', 'away_interception', 
                  'away_fumble_lost', 'away_fumble_forced', 'away_rush_attempt', 'away_pass_attempt', 
                  'away_pass_touchdown', 'away_qb_dropback', 'away_rush_touchdown', 'away_tackled_for_loss', 
                  'away_qb_hit', 'away_punt_attempt', 'away_kickoff_attempt', 'away_kickoff_inside_twenty', 
                  'away_penalty_yards', 'away_rushing_yards', 'away_passing_yards', 'away_receiving_yards', 
                  'away_yards_gained', 'away_sack', 'away_return_yards_positive', 'away_return_yards_negative', 
                  'away_yards_after_catch_positive', 'away_yards_after_catch_negative','away_pct','away_pf',
                  'away_pa','away_team_avg_score','away_net_rating','away_defense','away_offense',
                  'away_team_peformance','away_avg_passing_yards','away_avg_receiving_yards',
                  'away_avg_rushing_yards','away_avg_yards_gained']
    })

    # Add last 5 matches statistics for each feature
    # This creates rolling window features showing recent team performance
    for i in temp_df.itertuples():
        df = add_last5_stat(df,i.home, i.away)

    # Add glicko features
    logger.info(ADD_GLICKO_FEATURES_STARTED)
    df = add_glicko_features(df)

    logger.info(ADD_PREVIOUS_FEATURES_STARTED)
    temp_df = pd.DataFrame({
        "home" : ['total_home_score', 'total_home_epa', 
                  'total_home_rush_epa', 'total_home_pass_epa', 
                  'home_first_down_rush', 'home_first_down_pass', 
                  'home_third_down_converted', 'home_fourth_down_converted', 
                  'home_interception', 'home_fumble_lost', 'home_fumble_forced', 
                  'home_rush_attempt', 'home_pass_attempt', 'home_pass_touchdown', 
                  'home_qb_dropback', 'home_rush_touchdown', 'home_tackled_for_loss', 
                  'home_qb_hit', 'home_punt_attempt', 'home_kickoff_attempt', 
                  'home_kickoff_inside_twenty', 'home_penalty_yards', 
                  'home_rushing_yards', 'home_passing_yards', 'home_receiving_yards', 
                  'home_yards_gained', 'home_sack', 'home_return_yards_positive', 
                  'home_return_yards_negative', 'home_yards_after_catch_positive', 
                  'home_yards_after_catch_negative', 'home_pct',
 'home_pf',
 'home_pa',
 'home_team_avg_score',
 'home_net_rating',
 'home_defense',
 'home_offense',
 'home_team_peformance',
 'home_avg_passing_yards',
 'home_avg_receiving_yards',
 'home_avg_rushing_yards',
 'home_avg_yards_gained',
 'home_team_glicko_rating',
 'home_team_rd',
 'home_team_vol'],
        "away" : ['total_away_score', 'total_away_epa', 
                  'total_away_rush_epa', 'total_away_pass_epa', 
                  'away_first_down_rush', 'away_first_down_pass', 
                  'away_third_down_converted', 'away_fourth_down_converted', 
                  'away_interception', 'away_fumble_lost', 'away_fumble_forced', 
                  'away_rush_attempt', 'away_pass_attempt', 'away_pass_touchdown', 
                  'away_qb_dropback', 'away_rush_touchdown', 'away_tackled_for_loss', 
                  'away_qb_hit', 'away_punt_attempt', 'away_kickoff_attempt', 
                  'away_kickoff_inside_twenty', 'away_penalty_yards', 'away_rushing_yards', 
                  'away_passing_yards', 'away_receiving_yards', 'away_yards_gained', 'away_sack', 
                  'away_return_yards_positive', 'away_return_yards_negative', 
                  'away_yards_after_catch_positive', 'away_yards_after_catch_negative', 
                  'away_pct',
 'away_pf',
 'away_pa',
 'away_team_avg_score',
 'away_net_rating',
 'away_defense',
 'away_offense',
 'away_team_peformance',
 'away_avg_passing_yards',
 'away_avg_receiving_yards',
 'away_avg_rushing_yards',
 'away_avg_yards_gained',
 'away_team_glicko_rating',
 'away_team_rd',
 'away_team_vol']
    })

    

    # Add previous game features showing performance in the most recent previous game
    for i in temp_df.itertuples():
        df = add_prev_feature(df, i.home, i.away)

    # Add last 5 head to head win ratio of teams
    logger.info(ADD_LAST_5_H2H_WIN_RATIO_STARTED)
    df = add_last5_h2h_win_ratios(df)
    df = df[df["match_result"].isin([0, 1])]

    return df


def data_preprocessing(df):
    """
    Preprocess the feature-engineered DataFrame for model training.
    
    This function performs the final preprocessing steps:
    1. Label encodes categorical variables (teams, coaches, stadiums)
    2. Separates features (X) from target (y)
    3. Splits data into 80% training and 20% testing
    4. Scales numerical features using StandardScaler (fitted on train, applied to both)
    5. Applies SMOTE for class imbalance handling (only on training data)
    6. Returns preprocessed data and encoders for inference
    
    Args:
        df (pd.DataFrame): Feature-engineered DataFrame from feature_engineering()
        
    Returns:
        tuple: A tuple containing:
            - x_resampled (pd.DataFrame): Resampled and scaled training feature matrix
            - y_resampled (pd.DataFrame): Resampled training target vector (match_result)
            - scaler (StandardScaler): Fitted scaler for feature normalization
            - coach_le (LabelEncoder): Fitted encoder for coach names
            - team_le (LabelEncoder): Fitted encoder for team names
            - ground_le (LabelEncoder): Fitted encoder for stadium names
            - x_test (pd.DataFrame): Scaled test feature matrix
            - y_test (pd.Series): Test target vector
            
    Note:
        - Label encoders and scaler must be saved for use during inference
        - SMOTE is applied only to training data to handle class imbalance
        - Test data remains unscaled and unmodified for realistic evaluation
        - Only game-level features are kept (removes game_id, dates, etc.)
    """
    logger.info(DATA_PREPROCESSING_STARTED)
    
    # Step 1: Collect unique teams and coaches for label encoding
    teams = []
    coach = []
    for i in df[["home_team", "away_team", "home_coach", "away_coach"]].itertuples():
        if i.home_team not in teams:
            teams.append(i.home_team)
        
        if i.away_team not in teams:
            teams.append(i.away_team)
        
        if i.home_coach not in coach:
            coach.append(i.home_coach)

        if i.away_coach not in coach:
            coach.append(i.away_coach)
    
    # Initialize label encoders for categorical features
    team_le = LabelEncoder()
    coach_le = LabelEncoder()
    ground_le = LabelEncoder()

    # Fit encoders on all unique values
    team_le.fit(teams)
    coach_le.fit(coach)
    ground_le.fit(df["game_stadium"])

    # Step 2: Complete encoding
    logger.info(LABEL_ENCODING_STARTED)
    df["home_team"] = team_le.transform(df["home_team"])
    df["away_team"] = team_le.transform(df["away_team"])
    df["home_coach"] = coach_le.transform(df["home_coach"])
    df["away_coach"] = coach_le.transform(df["away_coach"])
    df["game_stadium"] = ground_le.transform(df["game_stadium"])

    # Step 3: Prepare the feature and target datasets
    # Remove columns that are identifiers, target leakages, or non-predictive
    remove_list = ["season",
'game_id',
 'total_home_score',
 'total_away_score',
 'total_home_epa',
 'total_away_epa',
 'total_home_rush_epa',
 'total_away_rush_epa',
 'total_home_pass_epa',
 'total_away_pass_epa',
 'home_first_down_rush',
 'home_first_down_pass',
 'home_third_down_converted',
 'home_fourth_down_converted',
 'home_interception',
 'home_fumble_lost',
 'home_fumble_forced',
 'home_rush_attempt',
 'home_pass_attempt',
 'home_pass_touchdown',
 'home_qb_dropback',
 'home_rush_touchdown',
 'home_tackled_for_loss',
 'home_qb_hit',
 'home_punt_attempt',
 'home_kickoff_attempt',
 'home_kickoff_inside_twenty',
 'home_penalty_yards',
 'home_rushing_yards',
 'home_passing_yards',
 'home_receiving_yards',
 'home_yards_gained',
 'home_sack',
 'away_first_down_rush',
 'away_first_down_pass',
 'away_third_down_converted',
 'away_fourth_down_converted',
 'away_interception',
 'away_fumble_lost',
 'away_fumble_forced',
 'away_rush_attempt',
 'away_pass_attempt',
 'away_pass_touchdown',
 'away_qb_dropback',
 'away_rush_touchdown',
 'away_tackled_for_loss',
 'away_qb_hit',
 'away_punt_attempt',
 'away_kickoff_attempt',
 'away_kickoff_inside_twenty',
 'away_penalty_yards',
 'away_rushing_yards',
 'away_passing_yards',
 'away_receiving_yards',
 'away_yards_gained',
 'away_sack',
 'home_return_yards_positive',
 'away_return_yards_positive',
 'home_return_yards_negative',
 'away_return_yards_negative',
 'home_yards_after_catch_positive',
 'away_yards_after_catch_positive',
 'home_yards_after_catch_negative',
 'away_yards_after_catch_negative', "match_result", 
"home_team_glicko_rating",
"away_team_glicko_rating",
"home_team_rd",
"away_team_rd",
"home_team_vol",
"away_team_vol","game_date",
"df_row",
"home_pf",
"home_pa",
"away_pf",
"away_pa",
 'home_defense',
 'away_defense',
 'home_offense',
 'away_offense',
 'home_net_rating',
 'away_net_rating',
  'home_pct',
 'away_pct',
 'home_team_avg_score',
 'away_team_avg_score',
 'home_avg_passing_yards',
 'away_avg_passing_yards',
 'home_avg_receiving_yards',
 'away_avg_receiving_yards',
 'home_avg_rushing_yards',
 'away_avg_rushing_yards',
 'home_avg_yards_gained',
 'away_avg_yards_gained',
  'league_avg_score_before',
 'home_team_peformance',
 'away_team_peformance',
 'league_avg_passing_yards_before',
 'league_avg_receiving_yards_before',
 'league_avg_rushing_yards_before',
 'league_avg_yards_gained_before']
    
    # Separate features (X) from target (y)
    X = df.drop(columns=remove_list)
    y = df["match_result"]

    # Step 4: Split data based on season (Train: < current_season, Test: == current_season)
    current_season = df['season'].max()
    logger.info(f"Splitting data based on time. Test season: {current_season}")
    
    # Create masks using the original df (which still has 'season')
    train_mask = df['season'] < current_season
    test_mask = df['season'] == current_season
    
    X_train = X[train_mask]
    y_train = y[train_mask]
    X_test = X[test_mask]
    y_test = y[test_mask]
    
    logger.info(f"Training set (1999-{current_season-1}): {X_train.shape[0]} samples")
    logger.info(f"Test set ({current_season}): {X_test.shape[0]} samples")

    # Step 5: Scale features (fit on train, transform both train and test)
    logger.info(SCALING_STARTED)
    scaler = StandardScaler()
    x_train_scaled = scaler.fit_transform(X_train)
    x_test_scaled = scaler.transform(X_test)  # Use transform, not fit_transform

    # Step 6: Apply SMOTE only to training data
    logger.info(SMOTE_STARTED)
    smote = SMOTEN(random_state=42)
    x_resampled_array, y_resampled_array = smote.fit_resample(x_train_scaled, y_train)
    
    logger.info(f"After SMOTE - Training set: {x_resampled_array.shape[0]} samples")

    # Convert back to DataFrame for consistency
    x_resampled = pd.DataFrame(x_resampled_array, columns=X.columns)
    y_resampled = pd.DataFrame(y_resampled_array, columns=['match_result'])
    x_test = pd.DataFrame(x_test_scaled, columns=X.columns)
    y_test = pd.Series(y_test, name='match_result')

    return x_resampled, y_resampled, scaler, coach_le, team_le, ground_le, x_test, y_test


def model_train(x_resampled, y_resampled, x_test=None, y_test=None):
    """
    Train a Random Forest classifier model on preprocessed NFL data.
    
    This function trains a RandomForestClassifier.
    The model is configured for binary classification to predict NFL game winners.
    
    Args:
        x_resampled (pd.DataFrame): Resampled and scaled feature matrix from data_preprocessing()
        y_resampled (pd.DataFrame): Resampled target vector (match_result: 0 or 1)
        x_test (pd.DataFrame, optional): Test feature matrix for evaluation
        y_test (pd.Series, optional): Test target vector for evaluation
        
    Returns:
        RandomForestClassifier: Trained Random Forest model ready for predictions
        
    Model Configuration:
        - n_estimators: 334
        - max_depth: 21
        - min_samples_split: 3
        - min_samples_leaf: 1
        - max_features: 'sqrt'
        - bootstrap: False
        - class_weight: 'balanced'
        
    Note:
        - Random seed is set for reproducibility
    """
    logger.info(MODEL_TRAINING_STARTED)
    
    y_train = _extract_target_series(y_resampled)
    
    # Initialize Random Forest classifier with optimized hyperparameters
    model = RandomForestClassifier(
        n_estimators=334, 
        max_depth=21, 
        min_samples_split=3, 
        min_samples_leaf=1, 
        max_features='sqrt', 
        bootstrap=False, 
        class_weight='balanced',
        random_state=42
    )
    
    # Train the model on preprocessed data
    model.fit(x_resampled, y_train)
    
    if x_test is not None and y_test is not None:
         logger.info("Test set provided but not used for training monitoring in Random Forest")

    return model
def _extract_target_series(target: Union[pd.Series, pd.DataFrame]) -> pd.Series:
    """Normalize target container into a Series."""
    if isinstance(target, pd.DataFrame):
        if target.empty:
            return target.squeeze()
        return target.iloc[:, 0]
    return target


def _prepare_eval_set(x_test, y_test):
    """
    Prepare an evaluation dataset tuple for API compatibility.

    This helper function converts the provided test features and labels
    into a standardized `(X_test, y_test)` tuple. It exists primarily
    to maintain a consistent training API across different model types,
    even though Random Forest models do not use evaluation sets or
    support early stopping.

    Parameters
    ----------
    x_test : array-like or pandas.DataFrame or None
        Test feature matrix. If not already a DataFrame, it will be
        converted into one.
    y_test : array-like or None
        Test target values. Will be converted into a NumPy array.

    Returns
    -------
    tuple or None
        Returns `(X_test, y_test)` when both inputs are provided.
        Returns `None` if either `x_test` or `y_test` is `None`.

    Notes
    -----
    - The returned value is not used during Random Forest training.
    - This function is retained for interface consistency with models
      such as CatBoost that support evaluation sets.
    - Shapes of the test data are logged for informational purposes.
    """
    if x_test is None or y_test is None:
        return None

    x_frame = x_test if isinstance(x_test, pd.DataFrame) else pd.DataFrame(x_test)
    y_array = _ensure_numpy_array(y_test)

    logger.info(f"Test set available: {x_frame.shape[0]} samples (not used in Random Forest training)")
    logger.info(f"   X_test shape: {x_frame.shape}, y_test shape: {y_array.shape}")
    return x_frame, y_array


def _ensure_numpy_array(values):
    """Convert supported containers into numpy arrays."""
    if isinstance(values, pd.DataFrame):
        if values.empty:
            return values.squeeze().values
        return values.iloc[:, 0].values
    if isinstance(values, pd.Series):
        return values.values
    import numpy as np  # noqa: E402
    return values if isinstance(values, np.ndarray) else np.array(values)

def get_prediction(model, input_data):
    """
    Generate win probability predictions for a single NFL matchup.

    This function uses a trained classification model to compute
    the predicted win probabilities for the home and away teams
    based on the provided input features.

    Parameters
    ----------
    model : sklearn-like classifier
        Trained classification model implementing `predict_proba`.
    input_data : pandas.DataFrame or array-like
        Preprocessed input feature data corresponding to a single game.

    Returns
    -------
    tuple of float
        Tuple containing:
        - home_probability : float
            Predicted probability of a home-team win.
        - away_probability : float
            Predicted probability of an away-team win.

    Notes
    -----
    - The model is assumed to be trained with the following class encoding:
        - Class 0 → Away team win
        - Class 1 → Home team win
    - `predict_proba` returns probabilities in the order:
      `[P(class 0), P(class 1)]`.
    - Intended for use in inference and prediction pipelines.
    """
    proba = model.predict_proba(input_data)[0]
    # Class 0 = away win, Class 1 = home win
    away_probability = proba[0]  # Probability of class 0 (away win)
    home_probability = proba[1]   # Probability of class 1 (home win)
    return home_probability, away_probability


def _load_model_and_encoders():
    """
    Load the trained model, scaler, and label encoders from disk.

    This helper function loads all serialized components required
    for inference, including the trained classification model,
    feature scaler, and label encoders for teams, coaches, and
    stadium/ground identifiers.

    Returns
    -------
    tuple
        A tuple containing:
        - model : trained classification model
        - scaler : fitted feature scaler
        - team_le : fitted label encoder for team names
        - coach_le : fitted label encoder for coach names
        - ground_le : fitted label encoder for stadium/ground names

    Notes
    -----
    - Objects are loaded from pickle files located in `models_path`.
    - Logging is performed before and after loading for traceability.
    - This function assumes that all required files exist and are
      compatible with the current runtime environment.
    - Intended for internal use during model inference setup.
    """
    logger.info(LOADING_MODEL_AND_ENCODERS)
    model_path = os.path.join(models_path, "cat_model.pkl")
    scaler_path = os.path.join(models_path, "scaler.pkl")
    team_encoder_path = os.path.join(models_path, "team_encoder.pkl")
    coach_encoder_path = os.path.join(models_path, "coach_encoder.pkl")
    ground_encoder_path = os.path.join(models_path, "ground_encoder.pkl")
    
    with open(model_path, 'rb') as f:
        model = pickle.load(f)
    with open(scaler_path, 'rb') as f:
        scaler = pickle.load(f)
    with open(team_encoder_path, 'rb') as f:
        team_le = pickle.load(f)
    with open(coach_encoder_path, 'rb') as f:
        coach_le = pickle.load(f)
    with open(ground_encoder_path, 'rb') as f:
        ground_le = pickle.load(f)
    
    logger.info(MODEL_AND_ENCODERS_LOADED)
    return model, scaler, team_le, coach_le, ground_le


def _connect_to_database():
    """Create and return database connection."""
    os.makedirs(os.path.dirname(database_path), exist_ok=True)
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    logger.info(CONNECTED_TO_DATABASE.format(database_path))
    return conn, cursor


def _encode_categorical_feature(encoder, value: str, feature_name: str) -> int:
    """Encode a single categorical feature with error handling."""
    try:
        return encoder.transform([value])[0]
    except (ValueError, KeyError):
        logger.warning(UNKNOWN_FEATURE_ENCODING.format(feature_name, value))
        return 0


def _encode_all_categoricals(input_df: pd.DataFrame, team_le, coach_le, ground_le) -> Dict[str, int]:
    """Encode all categorical features from input DataFrame."""
    original_home_team = input_df["home_team"].iloc[0]
    original_away_team = input_df["away_team"].iloc[0]
    original_home_coach = input_df["home_coach"].iloc[0]
    original_away_coach = input_df["away_coach"].iloc[0]
    original_game_stadium = input_df["game_stadium"].iloc[0]
    
    return {
        'home_team': _encode_categorical_feature(team_le, original_home_team, 'home_team'),
        'away_team': _encode_categorical_feature(team_le, original_away_team, 'away_team'),
        'home_coach': _encode_categorical_feature(coach_le, original_home_coach, 'home_coach'),
        'away_coach': _encode_categorical_feature(coach_le, original_away_coach, 'away_coach'),
        'game_stadium': _encode_categorical_feature(ground_le, original_game_stadium, 'game_stadium')
    }


def _prepare_feature_dataframe(input_df: pd.DataFrame, encoded_values: Dict[str, int]) -> pd.DataFrame:
    """Prepare feature DataFrame in correct order matching training data."""
    categorical_cols = ['game_stadium', 'home_team', 'away_team', 'home_coach', 'away_coach']
    numerical_cols = [col for col in input_df.columns if col not in categorical_cols]
    
    feature_dict = {
        'game_stadium': [encoded_values['game_stadium']],
        'home_team': [encoded_values['home_team']],
        'away_team': [encoded_values['away_team']],
        'home_coach': [encoded_values['home_coach']],
        'away_coach': [encoded_values['away_coach']]
    }
    
    for col in numerical_cols:
        feature_dict[col] = [input_df[col].iloc[0]]
    
    return pd.DataFrame(feature_dict)


def _process_single_game(game: Dict[str, Any], week: int, year: int, model, scaler, 
                         team_le, coach_le, ground_le, cursor, conn) -> bool:
    """
    Process a single NFL game, generate a prediction, and store it in the database.

    This function orchestrates the full inference pipeline for one game:
    it validates game metadata, generates model input features, encodes
    categorical variables, scales features, obtains win probabilities
    from the trained model, determines the predicted winner, and persists
    the result to the database.

    Parameters
    ----------
    game : dict
        Dictionary containing game metadata. Expected keys include:
        - 'game_id'
        - 'home_team'
        - 'away_team'
        - 'home_coach'
        - 'away_coach'
        - 'stadium'
        - 'home_team_logo_url'
        - 'away_team_logo_url'
    week : int
        Week number of the NFL season.
    year : int
        NFL season year.
    model : sklearn-like classifier
        Trained model implementing `predict_proba`.
    scaler : sklearn-like scaler
        Fitted feature scaler used to transform input features.
    team_le : LabelEncoder
        Fitted label encoder for team identifiers.
    coach_le : LabelEncoder
        Fitted label encoder for coach names.
    ground_le : LabelEncoder
        Fitted label encoder for stadium/ground names.
    cursor : sqlite3.Cursor
        Database cursor used to execute SQL commands.
    conn : sqlite3.Connection
        Active database connection used to commit the transaction.

    Returns
    -------
    bool
        `True` if the prediction was successfully generated and stored;
        `False` if the game was skipped due to missing or invalid data.

    Notes
    -----
    - Games missing essential identifiers (game ID or team names)
      are skipped.
    - All features are generated using leakage-safe, pre-game data.
    - Database insertion is committed immediately after prediction.
    - Intended for internal use within a batch prediction workflow.
    """
    game_id = str(game.get("game_id", ""))
    home_team = game.get("home_team", "")
    away_team = game.get("away_team", "")
    home_coach = game.get("home_coach", "")
    away_coach = game.get("away_coach", "")
    stadium = game.get("stadium", "")
    home_logo = game.get("home_team_logo_url", "")
    away_logo = game.get("away_team_logo_url", "")
    
    if not all([game_id, home_team, away_team]):
        logger.warning(SKIPPING_GAME_MISSING_DATA.format(game_id))
        return False
    
    logger.info(PROCESSING_GAME.format(away_team, home_team, game_id))
    
    # Check for undecided teams (e.g., TBD vs TBD) and force 50/50 prediction
    if home_team == "TBD" or away_team == "TBD":
        logger.info(f"Undecided teams (TBD) found for game {game_id}. Setting default 50/50 probability.")
        home_prob = 0.5
        away_prob = 0.5
    else:
        # Generate input features
        input_df = get_inputs(
            home_team=home_team,
            away_team=away_team,
            home_coach=home_coach,
            away_coach=away_coach,
            game_stadium=stadium
        )
        
        # Encode categorical features
        encoded_values = _encode_all_categoricals(input_df, team_le, coach_le, ground_le)
        
        # Prepare feature DataFrame
        feature_df = _prepare_feature_dataframe(input_df, encoded_values)
        
        # Filter features to match scaler's expected input
        if hasattr(scaler, 'feature_names_in_'):
            feature_df = feature_df[scaler.feature_names_in_]
        
        # Scale and predict
        feature_array = scaler.transform(feature_df)
        home_prob, away_prob = get_prediction(model, feature_array)
    
    # Determine winner
    predicted_result = home_team if home_prob > away_prob else away_team
    
    # Store in database
    prediction_data: Dict[str, Any] = {
        'game_id': game_id,
        'year': year,
        'week': week,
        'home_team': home_team,
        'away_team': away_team,
        'home_team_win_probability': float(home_prob),
        'away_team_win_probability': float(away_prob),
        'predicted_result': predicted_result,
        'home_team_image_url': home_logo or "",
        'away_team_image_url': away_logo or "",
        'home_coach': home_coach or "",
        'away_coach': away_coach or "",
        'stadium': stadium or ""
    }
    
    insert_prediction_data(cursor, insert_prediction_data_query, prediction_data)
    conn.commit()
    
    logger.info(PREDICTION_STORED.format(away_team, home_team, home_prob, away_prob, predicted_result))
    return True


def _process_week(week: int, year: int, model, scaler, team_le, coach_le, 
                 ground_le, cursor, conn) -> int:
    """
    Process all NFL games for a given week and store predictions.

    This function retrieves the scheduled games for the specified
    NFL week and season, iterates through each game, generates
    predictions using the trained model, and persists the results
    to the database.

    Parameters
    ----------
    week : int
        Week number of the NFL season.
    year : int
        NFL season year.
    model : sklearn-like classifier
        Trained model implementing `predict_proba`.
    scaler : sklearn-like scaler
        Fitted feature scaler used to transform input features.
    team_le : LabelEncoder
        Fitted label encoder for team identifiers.
    coach_le : LabelEncoder
        Fitted label encoder for coach names.
    ground_le : LabelEncoder
        Fitted label encoder for stadium/ground names.
    cursor : sqlite3.Cursor
        Database cursor used to execute SQL commands.
    conn : sqlite3.Connection
        Active database connection used to commit transactions.

    Returns
    -------
    int
        Number of games successfully processed and stored.

    Notes
    -----
    - Game schedules are retrieved using `get_team_details`.
    - Each game is processed independently using `_process_single_game`.
    - Failures in individual games are logged and do not interrupt
      processing of the remaining games.
    - Database commits are handled within the per-game processing step.
    - Intended for internal use within weekly batch prediction jobs.
    """
    logger.info(PROCESSING_WEEK.format(week, year))
    
    games = get_team_details(year, week)
    if not games:
        logger.warning(NO_GAMES_FOUND.format(year, week))
        return 0
    
    logger.info(FOUND_GAMES_FOR_WEEK.format(len(games), week))
    
    processed_count = 0
    for game in games:
        try:
            if _process_single_game(game, week, year, model, scaler, team_le, 
                                  coach_le, ground_le, cursor, conn):
                processed_count += 1
        except Exception as e:
            logger.exception(ERROR_PROCESSING_GAME.format(game.get('game_id', 'unknown'), e))
    
    logger.info(WEEK_COMPLETED.format(week, len(games)))
    return processed_count


def generate_weekly_predictions(year: int) -> None:
    """
    Generate and store NFL game predictions for an entire season.

    This function orchestrates the end-to-end batch prediction workflow
    for a given NFL season. It loads the trained model and preprocessing
    artifacts, iterates through all regular-season weeks, generates
    predictions for each scheduled game, and stores the results in
    the database.

    Parameters
    ----------
    year : int
        NFL season year for which predictions should be generated.

    Returns
    -------
    None

    Notes
    -----
    - Loads the trained model, feature scaler, and label encoders using
      `_load_model_and_encoders`.
    - Iterates through weeks 1–18 of the NFL regular season.
    - Game schedules are fetched using `get_team_details`.
    - Per-week processing is handled by `_process_week`, which in turn
      processes each game individually.
    - Database connections are established once and reused across weeks.
    - Errors in individual games or weeks are logged and do not halt
      the overall process.
    - All database resources are safely closed upon completion.
    - Intended for scheduled batch jobs or season-wide backfills.

    """
    logger.info(STARTING_WEEKLY_PREDICTIONS.format(year))
    
    # Load model, scaler, and encoders
    try:
        model, scaler, team_le, coach_le, ground_le = _load_model_and_encoders()
    except FileNotFoundError as e:
        logger.error(MODEL_OR_ENCODER_NOT_FOUND.format(e))
        raise
    except Exception as e:
        logger.error(ERROR_LOADING_MODEL_ENCODERS.format(e))
        raise
    
    # Connect to database
    try:
        conn, cursor = _connect_to_database()
    except sqlite3.Error as e:
        logger.error(DATABASE_CONNECTION_FAILED.format(e))
        raise
    
    # Process each week from 1 to 18
    total_games = 0
    try:
        for week in range(1, 22):
            try:
                processed = _process_week(week, year, model, scaler, team_le, 
                                         coach_le, ground_le, cursor, conn)
                total_games += processed
            except Exception as e:
                logger.exception(ERROR_PROCESSING_WEEK.format(week, e))
    finally:
        conn.close()
    
    logger.info(COMPLETED_PREDICTIONS.format(year, total_games))


def _has_existing_scores(cursor, game_id: str) -> bool:
    """
    Determine whether final scores for a game already exist in the database.

    This helper function queries the database for stored match scores
    associated with the given game ID and checks whether both the
    home and away scores are present.

    Parameters
    ----------
    cursor : sqlite3.Cursor
        Database cursor used to execute the score lookup query.
    game_id : str
        Unique identifier for the game to be checked.

    Returns
    -------
    bool
        `True` if both home and away scores already exist in the database;
        `False` otherwise.

    Notes
    -----
    - Scores are considered present only if **both** home and away scores
      are non-NULL.
    - Partial or missing score records result in `False`.
    - Intended for internal use to prevent overwriting existing results.
    """
    existing_scores = fetch_scores_from_db(cursor, fetch_match_scores_query, game_id)
    if existing_scores and len(existing_scores) > 0:
        home_score_existing = existing_scores[0][1]
        away_score_existing = existing_scores[0][2]
        if home_score_existing is not None and away_score_existing is not None:
            logger.debug(GAME_ALREADY_HAS_SCORES.format(game_id, home_score_existing, away_score_existing))
            return True
    return False


def _is_game_date_in_past(game_id: str, game_date_str: str, current_date: pd.Timestamp) -> bool:
    """
    Determine whether a game has already been played based on its date.

    This helper function compares the scheduled game date against the
    provided current date to determine whether the game occurred in
    the past. It is typically used to decide whether post-game data
    (such as final scores) should be fetched or updated.

    Parameters
    ----------
    game_id : str
        Unique identifier for the game, used for logging.
    game_date_str : str
        Scheduled game date in `YYYY-MM-DD` format.
    current_date : pandas.Timestamp
        Current timestamp used as the reference point for comparison.

    Returns
    -------
    bool
        `True` if the game date is strictly earlier than the current date;
        `False` otherwise or if the date cannot be parsed.

    Notes
    -----
    - Games scheduled for the current date or a future date are treated
      as not yet played.
    - Missing or invalid date strings result in `False`.
    - Date parsing and comparison errors are logged and handled safely.
    - Intended for internal use within post-game data update workflows.
    """
    if game_date_str is None:
        logger.debug(COULD_NOT_RETRIEVE_GAME_DATE.format(game_id))
        return False
    
    try:
        from datetime import datetime
        game_date = datetime.strptime(game_date_str, "%Y-%m-%d").date()
        current_date_only = current_date.date()
        
        if game_date >= current_date_only:
            logger.debug(GAME_SCHEDULED_FUTURE.format(game_id, game_date_str))
            return False
        return True
    except (ValueError, AttributeError) as e:
        logger.warning(COULD_NOT_PARSE_GAME_DATE.format(game_date_str, game_id, e))
        return False


def _validate_and_process_scores(game_id: str, scores: Dict[str, Any]) -> Tuple[Optional[int], Optional[int]]:
    """
    Validate and normalize game scores retrieved from an external API.

    This helper function verifies that both home and away scores are
    present and convertible to integers. It is used to ensure score
    data integrity before updating persistent storage or downstream
    analytics.

    Parameters
    ----------
    game_id : str
        Unique identifier for the game, used for logging.
    scores : dict
        Dictionary containing score data from the API response.
        Expected keys include:
        - 'home_score'
        - 'away_score'

    Returns
    -------
    tuple of (int or None, int or None)
        Tuple containing validated `(home_score, away_score)` values
        as integers. Returns `(None, None)` if the input is invalid,
        incomplete, or cannot be converted.

    Notes
    -----
    - Both scores must be present and valid; partial results are rejected.
    - Missing, malformed, or non-numeric score values are logged and
      handled gracefully.
    - Intended for internal use within score ingestion workflows.
    """
    if scores is None:
        logger.debug(MATCH_SCORES_UNAVAILABLE.format(game_id))
        return None, None
    
    home_score = scores.get("home_score")
    away_score = scores.get("away_score")
    
    if home_score is None or away_score is None:
        logger.warning(INCOMPLETE_SCORES.format(game_id, home_score, away_score))
        return None, None
    
    try:
        home_score = int(home_score)
        away_score = int(away_score)
        return home_score, away_score
    except (ValueError, TypeError):
        logger.warning(INVALID_SCORE_TYPES.format(game_id, home_score, away_score))
        return None, None


def _process_single_game_update(cursor, conn, game_id: str, current_date: pd.Timestamp) -> Tuple[int, int]:
    """
    Update final scores for a single NFL game if eligible.

    This helper function determines whether a game should be updated
    with final scores by performing a sequence of checks:
    - Verifies that scores are not already present in the database.
    - Confirms that the game date is in the past.
    - Fetches and validates final scores from the ESPN API.
    - Updates the database with the validated scores.

    Parameters
    ----------
    cursor : sqlite3.Cursor
        Database cursor used to execute SQL commands.
    conn : sqlite3.Connection
        Active database connection used to commit updates.
    game_id : str
        Unique identifier for the game to be processed.
    current_date : pandas.Timestamp
        Current timestamp used to determine whether the game
        has already been played.

    Returns
    -------
    tuple of (int, int)
        Tuple containing:
        - updated_count : int
            Number of games successfully updated (0 or 1).
        - skipped_count : int
            Number of games skipped due to existing scores,
            future game date, or invalid score data.

    Notes
    -----
    - Games with existing non-null scores are skipped.
    - Games scheduled for today or the future are skipped.
    - Partial or invalid score data is rejected.
    - Database updates are committed immediately upon success.
    - Intended for internal use within batch score-update workflows.
    """
    updated = 0
    skipped = 0
    
    # Check if scores already exist
    if _has_existing_scores(cursor, game_id):
        return updated, skipped + 1
    
    # Check game date
    logger.debug(CHECKING_GAME_DATE.format(game_id))
    game_date_str = get_game_date(game_id)
    
    if not _is_game_date_in_past(game_id, game_date_str, current_date):
        return updated, skipped + 1
    
    # Fetch and validate scores
    logger.info(FETCHING_SCORES_FOR_GAME.format(game_id, game_date_str))
    scores = get_match_scores(game_id)
    home_score, away_score = _validate_and_process_scores(game_id, scores)
    
    if home_score is None or away_score is None:
        return updated, skipped + 1
    
    # Update database
    update_actual_result(cursor, update_actual_result_query, game_id, home_score, away_score)
    conn.commit()
    logger.info(UPDATED_GAME.format(game_id, home_score, away_score))
    
    return updated + 1, skipped


def update_match_results():
    """
    Update final scores for a single NFL game if eligible.

    This helper function determines whether a game should be updated
    with final scores by performing a sequence of checks:
    - Verifies that scores are not already present in the database.
    - Confirms that the game date is in the past.
    - Fetches and validates final scores from the ESPN API.
    - Updates the database with the validated scores.

    Parameters
    ----------
    cursor : sqlite3.Cursor
        Database cursor used to execute SQL commands.
    conn : sqlite3.Connection
        Active database connection used to commit updates.
    game_id : str
        Unique identifier for the game to be processed.
    current_date : pandas.Timestamp
        Current timestamp used to determine whether the game
        has already been played.

    Returns
    -------
    tuple of (int, int)
        Tuple containing:
        - updated_count : int
            Number of games successfully updated (0 or 1).
        - skipped_count : int
            Number of games skipped due to existing scores,
            future game date, or invalid score data.

    Notes
    -----
    - Games with existing non-null scores are skipped.
    - Games scheduled for today or the future are skipped.
    - Partial or invalid score data is rejected.
    - Database updates are committed immediately upon success.
    - Intended for internal use within batch score-update workflows.
    """
    logger.info(STARTING_MATCH_RESULTS_UPDATE)
    
    current_date = pd.Timestamp.now()
    current_year = current_date.year
    weeks_to_check = list(range(1, 22))
    
    try:
        os.makedirs(os.path.dirname(database_path), exist_ok=True)
        conn = sqlite3.connect(database_path)
        cursor = conn.cursor()
        logger.info(CONNECTED_TO_DATABASE.format(database_path))
    except sqlite3.Error as e:
        logger.error(DATABASE_CONNECTION_FAILED.format(e))
        raise
    
    total_updated = 0
    total_skipped = 0
    total_errors = 0
    
    try:
        for week in weeks_to_check:
            try:
                logger.info(CHECKING_WEEK.format(week, current_year))
                predictions = fetch_predictions(cursor, fetch_data_query, current_year, week)
                
                if not predictions:
                    logger.debug(NO_PREDICTIONS_FOUND.format(current_year, week))
                    continue
                
                logger.info(FOUND_PREDICTIONS_FOR_WEEK.format(len(predictions), week))
                
                for prediction in predictions:
                    try:
                        game_id = str(prediction[0])
                        updated_count, skipped_count = _process_single_game_update(cursor, conn, game_id, current_date)
                        total_updated += updated_count
                        total_skipped += skipped_count
                    except Exception as e:
                        logger.exception(ERROR_PROCESSING_GAME_UPDATE.format(prediction[0], e))
                        total_errors += 1
                        continue
                
                logger.info(WEEK_PROCESSED.format(week, len(predictions)))
                
            except Exception as e:
                logger.exception(ERROR_PROCESSING_WEEK_UPDATE.format(week, e))
                continue
        
        logger.info(MATCH_RESULTS_UPDATE_COMPLETED.format(total_updated, total_skipped, total_errors))
        
        # Calculate and log prediction accuracy for previous weeks
        _log_prediction_accuracy(cursor, current_year)
        
    finally:
        conn.close()


def _log_prediction_accuracy(cursor, year: int) -> None:
    """
    Compute and log prediction accuracy for completed games in a season.

    This function evaluates model performance by comparing stored
    predictions against actual game outcomes for all completed games
    in the specified NFL season. Accuracy is computed on a per-week
    basis as well as across the entire season, and results are logged
    for monitoring and analysis.

    Parameters
    ----------
    cursor : sqlite3.Cursor
        Database cursor used to query prediction and score data.
    year : int
        NFL season year for which prediction accuracy is calculated.

    Returns
    -------
    None

    Notes
    -----
    - Only games with both `home_score` and `away_score` present are
      included in the accuracy calculation.
    - Predicted results are compared against the actual game winner.
    - Weekly accuracy statistics and overall accuracy are logged
      via the application logger.
    - Errors during evaluation are caught and logged without
      interrupting the application flow.
    - Intended for internal monitoring and model performance tracking
    """
    logger.info(CALCULATING_PREDICTION_ACCURACY)
    
    try:
        completed_games = _fetch_completed_games(cursor, year)
        if not completed_games:
            logger.info(PREDICTION_ACCURACY_NO_DATA)
            return

        week_stats, total_correct, total_games = _calculate_accuracy_stats(completed_games)
        _log_weekly_accuracy(week_stats)
        _log_overall_accuracy(total_correct, total_games)
    except Exception as e:
        logger.exception(f"Error calculating prediction accuracy: {e}")


def _fetch_completed_games(cursor, year: int):
    """
    Retrieve completed NFL games with recorded final scores for a season.

    This helper function queries the database for all games in the
    specified NFL season that have both home and away scores recorded,
    indicating that the games have been completed.

    Parameters
    ----------
    cursor : sqlite3.Cursor
        Database cursor used to execute the SQL query.
    year : int
        NFL season year for which completed games are retrieved.

    Returns
    -------
    list of tuple
        List of tuples representing completed games, ordered by week
        and game ID. Each tuple contains:
        - week
        - game_id
        - home_team
        - away_team
        - predicted_result
        - home_score
        - away_score

    Notes
    -----
    - Only games with non-NULL `home_score` and `away_score` values
      are included.
    - Results are ordered to support week-by-week accuracy analysis.
    - Intended for internal use in prediction evaluation workflows.
    """
    cursor.execute(
        """
            SELECT week, 
                   game_id,
                   home_team,
                   away_team,
                   predicted_result,
                   home_score,
                   away_score
            FROM match_predictions 
            WHERE year = ? 
              AND home_score IS NOT NULL 
              AND away_score IS NOT NULL
            ORDER BY week, game_id
        """,
        (year,),
    )
    return cursor.fetchall()


def _calculate_accuracy_stats(completed_games):
    """
    Calculate weekly and overall prediction accuracy statistics.

    This helper function compares predicted game outcomes against
    actual results for a collection of completed games and aggregates
    accuracy metrics on a per-week and season-wide basis.

    Parameters
    ----------
    completed_games : iterable of tuple
        Iterable containing completed game records. Each record is
        expected to include:
        - week
        - game_id
        - home_team
        - away_team
        - predicted_result
        - home_score
        - away_score

    Returns
    -------
    tuple
        A tuple containing:
        - week_stats : dict
            Mapping of week number to a dictionary with:
            - 'correct' : number of correct predictions
            - 'total' : total number of games
        - total_correct : int
            Total number of correct predictions across all weeks.
        - total_games : int
            Total number of games evaluated.

    Notes
    -----
    - Actual winners are determined using `_determine_actual_winner`.
    - Games with no clear winner (e.g., draws) are counted as incorrect.
    - Intended for internal use in model evaluation and logging.
    """
    week_stats: Dict[int, Dict[str, int]] = {}
    total_correct = 0
    total_games = 0
    
    for week, _, home_team, away_team, predicted_result, home_score, away_score in completed_games:
        actual_winner = _determine_actual_winner(home_team, away_team, home_score, away_score)
        is_correct = actual_winner is not None and predicted_result == actual_winner

        if week not in week_stats:
            week_stats[week] = {"correct": 0, "total": 0}
        
        week_stats[week]["total"] += 1
        total_games += 1
        
        if is_correct:
            week_stats[week]["correct"] += 1
            total_correct += 1
    
    return week_stats, total_correct, total_games


def _determine_actual_winner(home_team: str, away_team: str, home_score: int, away_score: int) -> Optional[str]:
    """Return the actual winning team name or None for ties."""
    if home_score > away_score:
        return home_team
    if away_score > home_score:
        return away_team
    return None


def _log_weekly_accuracy(week_stats: Dict[int, Dict[str, int]]) -> None:
    """Log accuracy for each individual week."""
    logger.info(PREDICTION_ACCURACY_STARTED)
    for week in sorted(week_stats.keys()):
        stats = week_stats[week]
        total = stats["total"]
        correct = stats["correct"]
        accuracy_pct = (correct / total * 100) if total > 0 else 0.0
        logger.info(PREDICTION_ACCURACY_WEEK.format(week, correct, total, accuracy_pct))

def _log_overall_accuracy(total_correct: int, total_games: int) -> None:
    """Log the overall accuracy summary."""
    overall_accuracy = (total_correct / total_games * 100) if total_games > 0 else 0.0
    logger.info(PREDICTION_ACCURACY_SUMMARY.format(total_correct, total_games, overall_accuracy))
    logger.info(PREDICTION_ACCURACY_COMPLETED)


def _is_game_date_in_future(game_id: str, game_date_str: str, current_date_only) -> bool:
    """
    Determine whether a game is scheduled for a future date.

    This helper function compares the scheduled game date against the
    provided current date to determine whether the game has not yet
    been played.

    Parameters
    ----------
    game_id : str
        Unique identifier for the game, used for logging.
    game_date_str : str
        Scheduled game date in `YYYY-MM-DD` format.
    current_date_only : datetime.date
        Current date used as the reference point for comparison.

    Returns
    -------
    bool
        `True` if the game date is strictly later than the current date;
        `False` otherwise or if the date cannot be parsed.

    Notes
    -----
    - Games scheduled for the current date or a past date are treated
      as not future games.
    - Missing or invalid date strings result in `False`.
    - Date parsing errors are logged and handled gracefully.
    - Intended for internal use within scheduling and update workflows.
    """
    if game_date_str is None:
        logger.debug(COULD_NOT_RETRIEVE_GAME_DATE_FUTURE.format(game_id))
        return False
    
    try:
        from datetime import datetime
        game_date = datetime.strptime(game_date_str, "%Y-%m-%d").date()
        
        if game_date <= current_date_only:
            logger.debug(GAME_PAST_OR_TODAY.format(game_id, game_date_str))
            return False
        return True
    except (ValueError, AttributeError) as e:
        logger.warning(COULD_NOT_PARSE_GAME_DATE_FUTURE.format(game_date_str, game_id, e))
        return False


def _update_single_future_prediction(cursor, conn, game_info: Dict[str, str], model_objects: Dict[str, Any]) -> bool:
    """
    Update win probabilities and predicted outcome for a future NFL game.

    This helper function recalculates prediction probabilities for a
    scheduled (future) game using the latest trained model and feature
    pipeline, then updates the corresponding record in the database.

    It is intended to be used when predictions need to be refreshed
    (e.g., roster changes, coaching updates, or periodic re-forecasting)
    before the game is played.

    Parameters
    ----------
    cursor : sqlite3.Cursor
        Database cursor used to execute SQL UPDATE statements.
    conn : sqlite3.Connection
        Active database connection used to commit the update.
    game_info : dict
        Dictionary containing game metadata. Expected keys include:
        - 'game_id' : str
        - 'home_team' : str
        - 'away_team' : str
        - 'home_coach' : str
        - 'away_coach' : str
        - 'stadium' : str
        - 'game_date_str' : str
    model_objects : dict
        Dictionary containing trained model artifacts:
        - 'model' : trained classification model
        - 'scaler' : fitted feature scaler
        - 'team_le' : team label encoder
        - 'coach_le' : coach label encoder
        - 'ground_le' : stadium/ground label encoder

    Returns
    -------
    bool
        `True` if the prediction was successfully updated and committed
        to the database.

    Notes
    -----
    - Feature generation is performed using `get_inputs` and is
      leakage-safe (historical data only).
    - Categorical variables are encoded using the provided encoders.
    - Win probabilities are generated using `predict_proba`.
    - The database update is committed immediately upon success.
    - Intended for internal use within future-game prediction update
      workflows.
    """
    try:
        game_id = game_info["game_id"]
        game_date_str = game_info["game_date_str"]
        home_team = game_info["home_team"]
        away_team = game_info["away_team"]
        home_coach = game_info["home_coach"]
        away_coach = game_info["away_coach"]
        stadium = game_info["stadium"]
        
        model = model_objects["model"]
        scaler = model_objects["scaler"]
        team_le = model_objects["team_le"]
        coach_le = model_objects["coach_le"]
        ground_le = model_objects["ground_le"]
        
        # NEW: If teams are undecided, set 50/50 probability and return early
        if home_team == "TBD" or away_team == "TBD":
            logger.info(f"Skipping prediction for game {game_id} (Teams undecided). Setting 50/50 probability.")
            update_probabilities(
                cursor,
                update_probabilities_query,
                game_id,
                0.5,
                0.5,
                "TBD",
                home_team,
                away_team,
                home_coach,
                away_coach,
                stadium,
                game_info.get("home_team_image_url", ""),
                game_info.get("away_team_image_url", "")
            )
            conn.commit()
            return True
        
        logger.info(UPDATING_PREDICTION_FOR_GAME.format(game_id, game_date_str))
        
        input_df = get_inputs(
            home_team=home_team,
            away_team=away_team,
            home_coach=home_coach,
            away_coach=away_coach,
            game_stadium=stadium
        )
        
        encoded_values = _encode_all_categoricals(input_df, team_le, coach_le, ground_le)
        feature_df = _prepare_feature_dataframe(input_df, encoded_values)
        
        # Filter features to match scaler's expected input
        if hasattr(scaler, 'feature_names_in_'):
            feature_df = feature_df[scaler.feature_names_in_]
            
        feature_array = scaler.transform(feature_df)
        home_prob, away_prob = get_prediction(model, feature_array)
        predicted_result = home_team if home_prob > away_prob else away_team
        
        update_probabilities(
            cursor,
            update_probabilities_query,
            game_id,
            float(home_prob),
            float(away_prob),
            predicted_result,
            home_team,
            away_team,
            home_coach,
            away_coach,
            stadium,
            game_info.get("home_team_image_url", ""),
            game_info.get("away_team_image_url", "")
        )
        conn.commit()
        logger.info(UPDATED_PREDICTION_FOR_GAME.format(game_id, home_prob, away_prob, predicted_result))
        return True
        
    except Exception as e:
        logger.error(ERROR_PROCESSING_GAME_FUTURE.format(game_info.get("game_id", "unknown"), e))
        return False



def get_live_game_info(game_id: str) -> Dict[str, str]:
    """
    Fetch live game information from ESPN, including teams and coaches.
    
    This function ensures that the prediction pipeline uses the most up-to-date
    team names and coach information, rather than stale data from the database.
    """
    url = f"https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/{game_id}"
    try:
        resp = requests.get(url, timeout=10).json()
        comp = resp["competitions"][0]
        competitors = comp["competitors"]
        date = comp["date"][:10]
        year = int(date[:4])
        
        # Adjust year for postseason (games in Jan/Feb belong to previous season year)
        if int(date[5:7]) <= 2:
            year -= 1
            
        home = next(c for c in competitors if c["homeAway"] == "home")
        away = next(c for c in competitors if c["homeAway"] == "away")
        
        # Fetch detailed team data to get logos and abbreviations
        # The 'team' field in competitors is a $ref link
        home_team_resp = requests.get(home["team"]["$ref"], timeout=10).json()
        away_team_resp = requests.get(away["team"]["$ref"], timeout=10).json()
        
        home_team_abbr = home_team_resp.get("abbreviation")
        away_team_abbr = away_team_resp.get("abbreviation")
        
        home_team_id = home_team_resp.get("id")
        away_team_id = away_team_resp.get("id")
        
        # Map abbreviations to standard team names used in model
        home_team_name = map_team(home_team_abbr)
        away_team_name = map_team(away_team_abbr)
        
        if not home_team_name: home_team_name = "TBD"
        if not away_team_name: away_team_name = "TBD"
        
        # Fetch Coaches
        home_coach_first, home_coach_last = get_coach_name(home_team_id, year)
        away_coach_first, away_coach_last = get_coach_name(away_team_id, year)
        
        home_coach = f"{home_coach_first} {home_coach_last}" if home_coach_first and home_coach_last else ""
        away_coach = f"{away_coach_first} {away_coach_last}" if away_coach_first and away_coach_last else ""

        return {
            "game_id": game_id,
            "home_team": home_team_name,
            "away_team": away_team_name,
            "home_coach": home_coach,
            "away_coach": away_coach,
            "stadium": comp.get("venue", {}).get("fullName", ""),
            "game_date_str": date,
            "home_team_image_url": home_team_resp.get("logos", [{}])[0].get("href", ""),
            "away_team_image_url": away_team_resp.get("logos", [{}])[0].get("href", "")
        }
    except Exception as e:
        logger.error(f"Error fetching live data for game {game_id}: {e}")
        return {
            "game_id": game_id,
            "home_team": "TBD",
            "away_team": "TBD",
            "home_coach": "",
            "away_coach": "",
            "stadium": "",
            "game_date_str": "",
            "home_team_image_url": "",
            "away_team_image_url": ""
        }

def update_future_predictions():
    """
    Refresh win probability predictions for future NFL games.

    This function identifies games in the database that are scheduled
    for a future date and recalculates their win probabilities using
    the latest trained model and feature pipeline. Updated probabilities
    and predicted outcomes are written back to the database.

    The update process includes:
    - Fetching all existing predictions from the database.
    - Determining each game's scheduled date via the ESPN API.
    - Identifying games that have not yet been played.
    - Recomputing win probabilities and predicted winners.
    - Persisting updated predictions to the database.

    Returns
    -------
    None

    Notes
    -----
    - Only games with dates strictly later than the current date are updated.
    - Games already played or with invalid/missing dates are skipped.
    - Uses the most recently trained model, scaler, and label encoders.
    - Database updates are committed per game to ensure persistence.
    - Errors in individual games are logged and do not interrupt
      processing of remaining games.
    - Intended for scheduled refresh jobs and pre-game forecast updates.
    """
    logger.info(STARTING_FUTURE_PREDICTIONS_UPDATE)
    
    # Get current date
    current_date = pd.Timestamp.now()
    current_date_only = current_date.date()
    
    # Load model, scaler, and encoders
    try:
        model, scaler, team_le, coach_le, ground_le = _load_model_and_encoders()
        logger.info(MODEL_AND_ENCODERS_LOADED)
    except FileNotFoundError as e:
        logger.error(MODEL_OR_ENCODER_NOT_FOUND.format(e))
        raise
    except Exception as e:
        logger.error(ERROR_LOADING_MODEL_ENCODERS.format(e))
        raise
    
    # Connect to database
    try:
        os.makedirs(os.path.dirname(database_path), exist_ok=True)
        conn = sqlite3.connect(database_path)
        cursor = conn.cursor()
        logger.info(CONNECTED_TO_DATABASE.format(database_path))
    except sqlite3.Error as e:
        logger.error(DATABASE_CONNECTION_FAILED.format(e))
        raise
    
    total_updated = 0
    total_skipped = 0
    total_errors = 0
    
    try:
        # Fetch all predictions from database
        logger.info(FETCHING_ALL_PREDICTIONS)
        all_predictions = fetch_all_predictions(cursor, fetch_all_predictions_query)
        logger.info(FOUND_PREDICTIONS_IN_DATABASE.format(len(all_predictions)))
        
        # Process each prediction
        for prediction in all_predictions:
            try:
                game_id = str(prediction[0])
                # We used to read home_team, away_team, etc from DB here.
                # But that data is stale (e.g. TBD vs TBD).
                # Instead, we now fetch live data.
                
                logger.debug(CHECKING_GAME_DATE.format(game_id))
                
                # Fetch fresh info (Teams, Coaches, Date, Stadium) from ESPN
                game_info = get_live_game_info(game_id)
                game_date_str = game_info.get("game_date_str", "")
                
                if not _is_game_date_in_future(game_id, game_date_str, current_date_only):
                    total_skipped += 1
                    continue
                
                model_objects = {
                    "model": model,
                    "scaler": scaler,
                    "team_le": team_le,
                    "coach_le": coach_le,
                    "ground_le": ground_le
                }
                
                _update_single_future_prediction(cursor, conn, game_info, model_objects)
                total_updated += 1
                    
            except Exception as e:
                logger.exception(ERROR_PROCESSING_GAME_FUTURE.format(prediction[0] if prediction else 'unknown', e))
                total_errors += 1
                continue
        
        logger.info(FUTURE_PREDICTIONS_UPDATE_COMPLETED.format(total_updated, total_skipped, total_errors))
        
    finally:
        conn.close()

# sync 1774962858153326040
# sync 1774962858889495117
# sync 1774962859557198707
# sys_sync_7df7821
# sys_sync_511c7925
