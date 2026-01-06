
import pandas as pd
import numpy as np
import os
from typing import Dict, Any, Optional

# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Adjust to point to backend root
BACKEND_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", ".."))
DATA_DIR = os.path.join(BACKEND_DIR, "data", "input")
INPUT_CSV_PATH = os.path.join(DATA_DIR, "step3_nfl_processed_data.csv")

def get_last_5_stats(df: pd.DataFrame, team: str, home_feature: str, away_feature: str) -> float:
    """
    Compute the average value of a statistic over a team’s last five games.

    This function selects the most recent five games played by the given
    team (either as home or away), extracts the specified feature from
    each game, and returns the mean of those values.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing game-level data. Must include:
        - 'home_team'
        - 'away_team'
        - 'game_date'
        - `home_feature`
        - `away_feature`
    team : str
        Team identifier for which the last-five-game statistic is computed.
    home_feature : str
        Column name containing the statistic value when the team played
        as the home team.
    away_feature : str
        Column name containing the statistic value when the team played
        as the away team.

    Returns
    -------
    float
        Mean value of the statistic across the team’s last five games.
        Returns 0.0 if no prior games are available.

    Notes
    -----
    - Games are sorted by `game_date` in descending order to identify
      the most recent matches.
    - Fewer than five games are used if the team has played less than
      five games.
    - The function does not distinguish between seasons.
    - Intended for feature engineering in predictive modeling.
    """
    games = df[(df["home_team"] == team) | (df["away_team"] == team)].sort_values("game_date", ascending=False).head(5)
    values = []
    for row in games.itertuples():
        if row.home_team == team:
            values.append(getattr(row, home_feature))
        else:
            values.append(getattr(row, away_feature))
    return float(np.mean(values)) if values else 0.0

def get_prev_stat(df: pd.DataFrame, team: str, home_feature: str, away_feature: str) -> float:
    """
    Retrieve the statistic value from a team’s most recent game.

    This function identifies the latest game played by the specified
    team (either as home or away), extracts the relevant feature value,
    and returns it.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing game-level data. Must include:
        - 'home_team'
        - 'away_team'
        - 'game_date'
        - `home_feature`
        - `away_feature`
    team : str
        Team identifier for which the previous-game statistic is retrieved.
    home_feature : str
        Column name containing the statistic value when the team played
        as the home team.
    away_feature : str
        Column name containing the statistic value when the team played
        as the away team.

    Returns
    -------
    float
        Statistic value from the team’s most recent game.
        Returns 0.0 if no prior games are available.

    Notes
    -----
    - Games are sorted by `game_date` in descending order to identify
      the most recent match.
    - The function returns a single value (not an average).
    - The function does not distinguish between seasons.
    - Intended for feature engineering and exploratory analysis.
    """
    games = df[(df["home_team"] == team) | (df["away_team"] == team)].sort_values("game_date", ascending=False).head(1)
    if games.empty: return 0.0
    row = games.iloc[0]
    if row.home_team == team:
        return float(getattr(row, home_feature))
    else:
        return float(getattr(row, away_feature))

def _load_and_filter_data(game_date: Optional[str] = None) -> pd.DataFrame:
    """
    Retrieve the statistic value from a team’s most recent game.

    This function identifies the latest game played by the specified
    team (either as home or away), extracts the relevant feature value,
    and returns it.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing game-level data. Must include:
        - 'home_team'
        - 'away_team'
        - 'game_date'
        - `home_feature`
        - `away_feature`
    team : str
        Team identifier for which the previous-game statistic is retrieved.
    home_feature : str
        Column name containing the statistic value when the team played
        as the home team.
    away_feature : str
        Column name containing the statistic value when the team played
        as the away team.

    Returns
    -------
    float
        Statistic value from the team’s most recent game.
        Returns 0.0 if no prior games are available.

    Notes
    -----
    - Games are sorted by `game_date` in descending order to identify
      the most recent match.
    - The function returns a single value (not an average).
    - The function does not distinguish between seasons.
    - Intended for feature engineering and exploratory analysis.
    """
    print(f"Loading data from {INPUT_CSV_PATH}...")
    if not os.path.exists(INPUT_CSV_PATH):
        raise FileNotFoundError(f"Input data not found at: {INPUT_CSV_PATH}")
        
    df = pd.read_csv(INPUT_CSV_PATH, low_memory=False)
    
    if game_date:
        if not np.issubdtype(df["game_date"].dtype, np.datetime64):
            df["game_date"] = pd.to_datetime(df["game_date"])
        df = df[df["game_date"] < pd.to_datetime(game_date)]
    return df

def _compute_basic_stats(df: pd.DataFrame, home_team: str, away_team: str, home_features: list, away_features: list) -> Dict[str, Any]:
    """
    Compute previous-game and last-five-game statistics for home and
    away teams.

    This helper function generates basic historical features for a
    given matchup by computing:
    - the most recent game value (`prev_*`)
    - the average over the last five games (`last_5_*`)
    
    for each specified pair of home and away feature columns.

    Parameters
    ----------
    df : pd.DataFrame
        Historical game-level DataFrame used to compute statistics.
        Must include:
        - 'home_team'
        - 'away_team'
        - 'game_date'
    home_team : str
        Identifier for the home team.
    away_team : str
        Identifier for the away team.
    home_features : list of str
        List of feature column names corresponding to home-team statistics.
    away_features : list of str
        List of feature column names corresponding to away-team statistics.
        Must be aligned positionally with `home_features`.

    Returns
    -------
    dict
        Dictionary mapping feature names to single-element lists
        containing computed values. Keys include:
        - `prev_<feature>`
        - `last_5_<feature>`

    Notes
    -----
    - Feature pairs are processed positionally using `zip`.
    - Features not present in `df` are skipped.
    - Returned values are wrapped in lists to support direct
      DataFrame construction.
    - Intended for internal use as part of a feature-generation pipeline.
    """
    stats: Dict[str, Any] = {}
    for h_col, a_col in zip(home_features, away_features):
        if h_col not in df.columns or a_col not in df.columns:
            continue
            
        stats[f"prev_{h_col}"] = [get_prev_stat(df, home_team, h_col, a_col)]
        stats[f"last_5_{h_col}"] = [get_last_5_stats(df, home_team, h_col, a_col)]
        
        stats[f"prev_{a_col}"] = [get_prev_stat(df, away_team, h_col, a_col)]
        stats[f"last_5_{a_col}"] = [get_last_5_stats(df, away_team, h_col, a_col)]
    return stats

def _compute_glicko_stats(df: pd.DataFrame, home_team: str, away_team: str) -> Dict[str, Any]:
    """
    Compute previous-game Glicko-2 statistics for home and away teams.

    This helper function extracts the most recent (previous-game)
    Glicko-2 metrics for the specified home and away teams from an
    existing DataFrame of historical games.

    The following Glicko-2 features are supported when present:
    - team rating
    - rating deviation (RD)
    - volatility

    Parameters
    ----------
    df : pd.DataFrame
        Historical game-level DataFrame containing Glicko-2 features.
    home_team : str
        Identifier for the home team.
    away_team : str
        Identifier for the away team.

    Returns
    -------
    dict
        Dictionary mapping feature names to single-element lists
        containing the previous-game values. Possible keys include:
        - `prev_home_team_glicko_rating`
        - `prev_home_team_rd`
        - `prev_home_team_vol`
        - `prev_away_team_glicko_rating`
        - `prev_away_team_rd`
        - `prev_away_team_vol`

    Notes
    -----
    - Only columns present in the input DataFrame are processed.
    - Values are computed using `get_prev_stat`.
    - Returned values are wrapped in lists to support direct
      DataFrame construction.
    - Intended for internal use within a feature-engineering pipeline.
    """
    stats: Dict[str, Any] = {}
    glicko_suffixes = ['team_glicko_rating', 'team_rd', 'team_vol']
    for suffix in glicko_suffixes:
        h_col = f"home_{suffix}"
        a_col = f"away_{suffix}"
        if h_col in df.columns:
             stats[f"prev_home_{suffix}"] = [get_prev_stat(df, home_team, h_col, a_col)]
             stats[f"prev_away_{suffix}"] = [get_prev_stat(df, away_team, h_col, a_col)]
    return stats

def _compute_h2h_stats(df: pd.DataFrame, home_team: str, away_team: str) -> Dict[str, Any]:
    """
    Compute head-to-head win ratios for home and away teams based on
    recent matchups.

    This helper function identifies the most recent head-to-head games
    between the specified home and away teams (regardless of venue),
    considers up to the last five encounters, and computes win ratios
    for each team.

    Parameters
    ----------
    df : pd.DataFrame
        Historical game-level DataFrame. Must include:
        - 'home_team'
        - 'away_team'
        - 'game_date'
        - 'total_home_score'
        - 'total_away_score'
    home_team : str
        Identifier for the home team in the current matchup.
    away_team : str
        Identifier for the away team in the current matchup.

    Returns
    -------
    dict
        Dictionary containing head-to-head win ratio features wrapped
        in single-element lists:
        - `h2h_home_win_ratio` : Fraction of wins by the home team in
          the most recent head-to-head games.
        - `h2h_away_win_ratio` : Fraction of wins by the away team in
          the most recent head-to-head games.

    Notes
    -----
    - Up to the last five head-to-head games are considered.
    - Games are sorted by `game_date` in descending order.
    - Draws count toward the total number of games but do not
      increment either team’s win count.
    - If no prior head-to-head games exist, both ratios are set to 0.0.
    - Returned values are wrapped in lists to support direct
      DataFrame construction.
    """
    matches = df[((df["home_team"] == home_team) & (df["away_team"] == away_team)) |
                 ((df["home_team"] == away_team) & (df["away_team"] == home_team))] \
                 .sort_values("game_date", ascending=False).head(5)
    
    h_wins = 0
    a_wins = 0
    count = 0 
    for m in matches.itertuples():
        count += 1
        if m.total_home_score > m.total_away_score: w = m.home_team
        elif m.total_away_score > m.total_home_score: w = m.away_team
        else: w = "draw"
        
        if w == home_team: h_wins += 1
        if w == away_team: a_wins += 1
            
    return {
        "h2h_home_win_ratio": [h_wins/count if count > 0 else 0.0],
        "h2h_away_win_ratio": [a_wins/count if count > 0 else 0.0]
    }

def get_inputs(home_team: str, away_team: str, home_coach: str, away_coach: str, game_stadium: str, game_date: Optional[str] = None) -> pd.DataFrame:
    """
    Build a single-row, model-ready feature DataFrame for an NFL matchup.

    This function assembles all required pre-game input features for a
    specified home–away matchup. It loads historical data, computes
    statistical features (previous-game, last-5-game, Glicko ratings,
    and head-to-head metrics), appends categorical metadata, and returns
    a DataFrame with columns ordered exactly as expected by the model.

    All computed statistics are **leakage-safe**:
    - Previous-game features use only the most recent completed game.
    - Last-5 features use strictly historical games.
    - Glicko features are taken from the most recent prior rating state.
    - Head-to-head features use only past matchups.

    Parameters
    ----------
    home_team : str
        Identifier of the home team.
    away_team : str
        Identifier of the away team.
    home_coach : str
        Full name of the home team’s head coach.
    away_coach : str
        Full name of the away team’s head coach.
    game_stadium : str
        Name of the stadium where the game is played.
    game_date : str or None, optional
        Game date in ISO format (`YYYY-MM-DD`). Used to filter historical
        data so that only games played before this date are considered.
        If None, all available historical data is used.

    Returns
    -------
    pd.DataFrame
        A single-row DataFrame containing all engineered features in the
        exact column order required by the prediction model.

    Notes
    -----
    - Internally uses the following helper functions:
        - `_load_and_filter_data`
        - `_compute_basic_stats`
        - `_compute_glicko_stats`
        - `_compute_h2h_stats`
    - Missing features are filled with `0.0` to ensure schema stability.
    - The final column order is strictly enforced via `expected_order`.
    - Intended for real-time inference and batch prediction pipelines.
    """
    print(f"Generating inputs for {home_team} vs {away_team}...")
    
    # 1. Load Data
    df = _load_and_filter_data(game_date)

    input_data: Dict[str, Any] = {}

    # 2. Define Features
    home_features = ['total_home_score', 'total_home_epa', 'total_home_rush_epa', 'total_home_pass_epa', 'home_first_down_rush', 'home_first_down_pass', 'home_third_down_converted', 'home_fourth_down_converted', 'home_interception', 'home_fumble_lost', 'home_fumble_forced', 'home_rush_attempt', 'home_pass_attempt', 'home_pass_touchdown', 'home_qb_dropback', 'home_rush_touchdown', 'home_tackled_for_loss', 'home_qb_hit', 'home_punt_attempt', 'home_kickoff_attempt', 'home_kickoff_inside_twenty', 'home_penalty_yards', 'home_rushing_yards', 'home_passing_yards', 'home_receiving_yards', 'home_yards_gained', 'home_sack', 'home_return_yards_positive', 'home_return_yards_negative', 'home_yards_after_catch_positive', 'home_yards_after_catch_negative','home_pct', 'home_pf', 'home_pa', 'home_team_avg_score', 'home_net_rating', 'home_defense', 'home_offense', 'home_team_peformance', 'home_avg_passing_yards', 'home_avg_receiving_yards', 'home_avg_rushing_yards', 'home_avg_yards_gained']
    
    away_features = ['total_away_score', 'total_away_epa', 'total_away_rush_epa', 'total_away_pass_epa', 'away_first_down_rush', 'away_first_down_pass', 'away_third_down_converted', 'away_fourth_down_converted', 'away_interception', 'away_fumble_lost', 'away_fumble_forced', 'away_rush_attempt', 'away_pass_attempt', 'away_pass_touchdown', 'away_qb_dropback', 'away_rush_touchdown', 'away_tackled_for_loss', 'away_qb_hit', 'away_punt_attempt', 'away_kickoff_attempt', 'away_kickoff_inside_twenty', 'away_penalty_yards', 'away_rushing_yards', 'away_passing_yards', 'away_receiving_yards', 'away_yards_gained', 'away_sack', 'away_return_yards_positive', 'away_return_yards_negative', 'away_yards_after_catch_positive', 'away_yards_after_catch_negative','away_pct', 'away_pf', 'away_pa', 'away_team_avg_score', 'away_net_rating', 'away_defense', 'away_offense', 'away_team_peformance', 'away_avg_passing_yards', 'away_avg_receiving_yards', 'away_avg_rushing_yards', 'away_avg_yards_gained']

    # 3. Compute Stats
    input_data.update(_compute_basic_stats(df, home_team, away_team, home_features, away_features))
    input_data.update(_compute_glicko_stats(df, home_team, away_team))
    input_data.update(_compute_h2h_stats(df, home_team, away_team))

    # 4. Add Categoricals
    input_data['home_team'] = [home_team]
    input_data['away_team'] = [away_team]
    input_data['home_coach'] = [home_coach]
    input_data['away_coach'] = [away_coach]
    input_data['game_stadium'] = [game_stadium]

    input_df = pd.DataFrame(input_data)

    # 5. Final Reordering
    expected_order = [
        'game_stadium', 'home_team', 'away_team', 'home_coach', 'away_coach',
        'last_5_total_home_score', 'last_5_total_away_score', 'last_5_total_home_epa', 'last_5_total_away_epa',
        'last_5_total_home_rush_epa', 'last_5_total_away_rush_epa', 'last_5_total_home_pass_epa', 'last_5_total_away_pass_epa',
        'last_5_home_first_down_rush', 'last_5_away_first_down_rush', 'last_5_home_first_down_pass', 'last_5_away_first_down_pass',
        'last_5_home_third_down_converted', 'last_5_away_third_down_converted', 'last_5_home_fourth_down_converted', 'last_5_away_fourth_down_converted',
        'last_5_home_interception', 'last_5_away_interception', 'last_5_home_fumble_lost', 'last_5_away_fumble_lost',
        'last_5_home_fumble_forced', 'last_5_away_fumble_forced', 'last_5_home_rush_attempt', 'last_5_away_rush_attempt',
        'last_5_home_pass_attempt', 'last_5_away_pass_attempt', 'last_5_home_pass_touchdown', 'last_5_away_pass_touchdown',
        'last_5_home_qb_dropback', 'last_5_away_qb_dropback', 'last_5_home_rush_touchdown', 'last_5_away_rush_touchdown',
        'last_5_home_tackled_for_loss', 'last_5_away_tackled_for_loss', 'last_5_home_qb_hit', 'last_5_away_qb_hit',
        'last_5_home_punt_attempt', 'last_5_away_punt_attempt', 'last_5_home_kickoff_attempt', 'last_5_away_kickoff_attempt',
        'last_5_home_kickoff_inside_twenty', 'last_5_away_kickoff_inside_twenty', 'last_5_home_penalty_yards', 'last_5_away_penalty_yards',
        'last_5_home_rushing_yards', 'last_5_away_rushing_yards', 'last_5_home_passing_yards', 'last_5_away_passing_yards',
        'last_5_home_receiving_yards', 'last_5_away_receiving_yards', 'last_5_home_yards_gained', 'last_5_away_yards_gained',
        'last_5_home_sack', 'last_5_away_sack', 'last_5_home_return_yards_positive', 'last_5_away_return_yards_positive',
        'last_5_home_return_yards_negative', 'last_5_away_return_yards_negative', 'last_5_home_yards_after_catch_positive', 'last_5_away_yards_after_catch_positive',
        'last_5_home_yards_after_catch_negative', 'last_5_away_yards_after_catch_negative', 'last_5_home_pct', 'last_5_away_pct',
        'last_5_home_pf', 'last_5_away_pf', 'last_5_home_pa', 'last_5_away_pa', 'last_5_home_team_avg_score', 'last_5_away_team_avg_score',
        'last_5_home_net_rating', 'last_5_away_net_rating', 'last_5_home_defense', 'last_5_away_defense',
        'last_5_home_offense', 'last_5_away_offense', 'last_5_home_team_peformance', 'last_5_away_team_peformance',
        'last_5_home_avg_passing_yards', 'last_5_away_avg_passing_yards', 'last_5_home_avg_receiving_yards', 'last_5_away_avg_receiving_yards',
        'last_5_home_avg_rushing_yards', 'last_5_away_avg_rushing_yards', 'last_5_home_avg_yards_gained', 'last_5_away_avg_yards_gained',
        'prev_total_home_score', 'prev_total_away_score', 'prev_total_home_epa', 'prev_total_away_epa',
        'prev_total_home_rush_epa', 'prev_total_away_rush_epa', 'prev_total_home_pass_epa', 'prev_total_away_pass_epa',
        'prev_home_first_down_rush', 'prev_away_first_down_rush', 'prev_home_first_down_pass', 'prev_away_first_down_pass',
        'prev_home_third_down_converted', 'prev_away_third_down_converted', 'prev_home_fourth_down_converted', 'prev_away_fourth_down_converted',
        'prev_home_interception', 'prev_away_interception', 'prev_home_fumble_lost', 'prev_away_fumble_lost',
        'prev_home_fumble_forced', 'prev_away_fumble_forced', 'prev_home_rush_attempt', 'prev_away_rush_attempt',
        'prev_home_pass_attempt', 'prev_away_pass_attempt', 'prev_home_pass_touchdown', 'prev_away_pass_touchdown',
        'prev_home_qb_dropback', 'prev_away_qb_dropback', 'prev_home_rush_touchdown', 'prev_away_rush_touchdown',
        'prev_home_tackled_for_loss', 'prev_away_tackled_for_loss', 'prev_home_qb_hit', 'prev_away_qb_hit',
        'prev_home_punt_attempt', 'prev_away_punt_attempt', 'prev_home_kickoff_attempt', 'prev_away_kickoff_attempt',
        'prev_home_kickoff_inside_twenty', 'prev_away_kickoff_inside_twenty', 'prev_home_penalty_yards', 'prev_away_penalty_yards',
        'prev_home_rushing_yards', 'prev_away_rushing_yards', 'prev_home_passing_yards', 'prev_away_passing_yards',
        'prev_home_receiving_yards', 'prev_away_receiving_yards', 'prev_home_yards_gained', 'prev_away_yards_gained',
        'prev_home_sack', 'prev_away_sack', 'prev_home_return_yards_positive', 'prev_away_return_yards_positive',
        'prev_home_return_yards_negative', 'prev_away_return_yards_negative', 'prev_home_yards_after_catch_positive', 'prev_away_yards_after_catch_positive',
        'prev_home_yards_after_catch_negative', 'prev_away_yards_after_catch_negative', 'prev_home_pct', 'prev_away_pct',
        'prev_home_pf', 'prev_away_pf', 'prev_home_pa', 'prev_away_pa', 'prev_home_team_avg_score', 'prev_away_team_avg_score',
        'prev_home_net_rating', 'prev_away_net_rating', 'prev_home_defense', 'prev_away_defense',
        'prev_home_offense', 'prev_away_offense', 'prev_home_team_peformance', 'prev_away_team_peformance',
        'prev_home_avg_passing_yards', 'prev_away_avg_passing_yards', 'prev_home_avg_receiving_yards', 'prev_away_avg_receiving_yards',
        'prev_home_avg_rushing_yards', 'prev_away_avg_rushing_yards', 'prev_home_avg_yards_gained', 'prev_away_avg_yards_gained',
        'prev_home_team_glicko_rating', 'prev_away_team_glicko_rating', 'prev_home_team_rd', 'prev_away_team_rd',
        'prev_home_team_vol', 'prev_away_team_vol', 'h2h_home_win_ratio', 'h2h_away_win_ratio'
    ]
    
    # Fill missing with 0 just in case and reorder
    for col in expected_order:
        if col not in input_df.columns:
            # print(f"Warning: Missing column {col}, filling with 0")
            input_df[col] = 0.0
            
    return input_df[expected_order]
# sync 1774962858079275190
# sys_sync_16112cbb
# sys_sync_442b349
