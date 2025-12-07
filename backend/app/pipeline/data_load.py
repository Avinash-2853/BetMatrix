import nflreadpy as nfl
from dotenv import load_dotenv
import os
from typing import List, Optional
import pandas as pd


_ = load_dotenv()
data_path = os.getenv("save_data_path")
if not data_path:
    # Default data path relative to the backend directory
    backend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
    data_path = os.path.join(backend_dir, "data", "input")


def load_nfl_data(seasons: List[int], output_file: Optional[str] = None) -> pd.DataFrame:
    """
    Load and preprocess NFL play-by-play data for selected seasons.

    This function downloads NFL play-by-play data for the specified
    seasons, selects a predefined subset of relevant game, team,
    and play-level features, and returns the filtered DataFrame.

    .. deprecated::
        The `output_file` parameter is deprecated and currently ignored.

    Parameters
    ----------
    seasons : list of int
        List of NFL seasons to load (e.g., [2020, 2021, 2022]).
    output_file : str or None, optional
        Deprecated. This parameter is ignored and has no effect.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing NFL play-by-play data restricted to
        the available columns from the predefined feature list.

    Notes
    -----
    - Data is loaded using `nfl.load_pbp`.
    - Only columns present in the source data are retained; missing
      columns are silently ignored.
    - The function does not perform feature engineering beyond
      column selection.
    - Column availability may vary by season and data source version.

    See Also
    --------
    nfl.load_pbp : Function used to load NFL play-by-play data.
    """

    # Load play-by-play data
    df = nfl.load_pbp(seasons)

    # List of features you want
    features = [
        "game_id",
        "play_id",
"season",
"week",
"season_type",
"game_date",
"stadium", 
"game_stadium",
"roof",
"surface",
"location",
"temp",
"wind",
"home_team",
"away_team",
"home_coach",
"away_coach",
"Div_game",
"home_score",
"away_score",
"total_home_score",
"total_away_score",
"posteam",
"posteam_score",
"defteam_score",
"score_differential",
"first_down_rush",
"first_down_pass",
"third_down_converted",
"fourth_down_converted",
"sack",
"interception",
"fumble_lost",
"penalty_yards",
"rush_attempt",
"pass_attempt",
"rushing_yards",
"passing_yards",
"receiving_yards",
"pass_touchdown",
"qb_dropback",
"rush_touchdown",
"yards_after_catch",
"tackled_for_loss",
"qb_hit",
"fumble_forced",
"fumble_recovery_1_yards",
"field_goal_attempt",
"field_goal_result",
"extra_point_attempt",
"extra_point_result",
"punt_attempt",
"kickoff_attempt",
"kickoff_inside_twenty",
"return_yards",
"spread_line",
"total_line",
"vegas_wp",
"Vegas_home_wp",
"total_home_epa",
"total_away_epa",
"total_home_rush_epa",
"total_away_rush_epa",
"total_home_pass_epa",
"total_away_pass_epa",
"total_home_comp_air_epa",
"total_away_comp_air_epa",
"total_home_comp_yac_epa",
"total_away_comp_yac_epa",
'total_home_raw_air_epa',
"total_away_raw_air_epa",
"total_home_raw_yac_epa",
"Total_away_raw_yac_epa",
"total_home_comp_air_wpa",
"total_away_comp_air_wpa",
"total_home_comp_yac_wpa",
"total_away_comp_yac_wpa",
"total_home_raw_air_wpa",
"total_away_raw_air_wpa",
"total_home_raw_yac_wpa",
"total_away_raw_yac_wpa",
"quarter_seconds_remaining",
"half_seconds_remaining",
"game_seconds_remaining",
"yards_gained"
    ]

    # Keep only available columns
    available_features: List[str] = [f for f in features if f in df.columns]  # type: ignore
    df = df[available_features]

    return df  # type: ignore
# sync 1774962858434106315
# sync 1774962858564977424
# sys_sync_12c06c9e
# sys_sync_1727340b
