# type: ignore
import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Any
from collections import defaultdict

def add_match_result(
    df: pd.DataFrame,
    home_col: str = "total_home_score",
    away_col: str = "total_away_score",
    result_col: str = "match_result"
) -> pd.DataFrame:
    """
    Add a match result column to a DataFrame.

    - 1 → Home team wins
    - 0 → Away team wins
    - 0.5 → Draw

    Parameters:
    df (pd.DataFrame): Input DataFrame
    home_col (str): Column name for home team score
    away_col (str): Column name for away team score
    result_col (str): Name of the new result column

    Returns:
    pd.DataFrame: DataFrame with the new result column
    """
    df[result_col] = np.where(
        df[home_col] > df[away_col], 1,
        np.where(df[home_col] < df[away_col], 0, 0.5)
    )
    return df


def add_last5_stat(df: pd.DataFrame, home_feature: str, away_feature: str) -> pd.DataFrame:
    """
    Add rolling last-5-game average features for home and away teams.

    For each game (row), this function computes the mean of the specified
    feature over the **previous five games** played by the home and away
    teams. The computed rolling averages are added as new columns:
    `last_5_<home_feature>` and `last_5_<away_feature>`.

    If a team has played fewer than five prior games, the mean is computed
    using all available past games. For a team’s first appearance, the
    rolling value defaults to 0.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing game-level data. Must include the
        following columns:
        - 'home_team'
        - 'away_team'
        - `home_feature`
        - `away_feature`
    home_feature : str
        Column name representing the home team’s feature value for the game.
    away_feature : str
        Column name representing the away team’s feature value for the game.

    Returns
    -------
    pd.DataFrame
        The input DataFrame augmented with two new columns:
        - `last_5_<home_feature>` : Rolling mean of the home feature over
          the team’s previous five games.
        - `last_5_<away_feature>` : Rolling mean of the away feature over
          the team’s previous five games.

    Notes
    -----
    - The DataFrame **must be sorted chronologically** (oldest to newest)
      so that rolling statistics are computed correctly.
    - Rolling values are calculated independently for each team.
    - The function mutates and returns the input DataFrame.
    - Initial values are seeded with 0 for teams with no prior games.
    """
    teams: Dict[str, List[float]] = {}
    home_team: List[float] = []
    away_team: List[float] = []

    for i in df[["home_team", "away_team", home_feature, away_feature]].itertuples():
        if i.home_team not in teams:
            teams[i.home_team] = [0]  # type: ignore

        if i.away_team not in teams:
            teams[i.away_team] = [0]  # type: ignore

        if i.home_team in teams:
            home_team.append(np.mean(teams[i.home_team][-5:]))  # type: ignore
            teams[i.home_team].append(getattr(i, home_feature))  # type: ignore

        if i.away_team in teams:
            away_team.append(np.mean(teams[i.away_team][-5:]))  # type: ignore
            teams[i.away_team].append(getattr(i, away_feature))  # type: ignore

    df[f"last_5_{home_feature}"] = home_team
    df[f"last_5_{away_feature}"] = away_team

    return df



def add_prev_feature(df: pd.DataFrame, home_feature: str, away_feature: str) -> pd.DataFrame:
    """
    Add previous-game feature values for home and away teams.

    For each game (row), this function retrieves the most recent value
    of the specified feature from the **immediately preceding game**
    played by the home and away teams. The retrieved values are stored
    in new columns prefixed with `prev_`.

    If a team has no prior game in the dataset, the missing previous
    value is imputed using the mean of the corresponding feature
    across all games.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing game-level data. Must include the
        following columns:
        - 'home_team'
        - 'away_team'
        - `home_feature`
        - `away_feature`
    home_feature : str
        Column name representing the home team’s feature value for the game.
    away_feature : str
        Column name representing the away team’s feature value for the game.

    Returns
    -------
    pd.DataFrame
        The input DataFrame augmented with two new columns:
        - `prev_<home_feature>` : Feature value from the home team’s
          immediately previous game.
        - `prev_<away_feature>` : Feature value from the away team’s
          immediately previous game.

    Notes
    -----
    - The DataFrame **must be sorted chronologically** (oldest to newest)
      for previous-game tracking to be correct.
    - Previous values are tracked independently for each team.
    - Missing values (e.g., first game of a team) are imputed using
      the dataset-wide mean of the corresponding feature.
    - The function resets the DataFrame index before processing.
    - The function mutates and returns the input DataFrame.
    """
    teams: Dict[str, Any] = {}
    home_list: List[Any] = []
    away_list: List[Any] = []
    df = df.reset_index(drop=True)

    for i in df[["home_team","away_team", home_feature, away_feature]].itertuples():
        
        if i.home_team not in teams:
            teams[i.home_team] = np.nan  # type: ignore
        
        if i.away_team not in teams:
            teams[i.away_team] = np.nan  # type: ignore

        if i.home_team in teams:
            home_list.append(teams[i.home_team])  # type: ignore
            teams[i.home_team] = getattr(i, home_feature)  # type: ignore

        if i.away_team in teams:
            away_list.append(teams[i.away_team])  # type: ignore
            teams[i.away_team] = getattr(i, away_feature)  # type: ignore

    df[f"prev_{home_feature}"] = home_list
    df[f"prev_{away_feature}"] = away_list


    df[f"prev_{home_feature}"] = df[f"prev_{home_feature}"].fillna(np.mean(df[home_feature])) # type: ignore
    df[f"prev_{away_feature}"] = df[f"prev_{away_feature}"].fillna(np.mean(df[away_feature]))  # type: ignore

    return df




def add_last5_h2h_win_ratios(df: pd.DataFrame, date_col: Optional[str] = None, n: int = 5) -> pd.DataFrame:
    """
    Compute rolling head-to-head (H2H) win ratios over the last N matchups
    between two teams.

    For each game, this function calculates the fraction of wins achieved
    by the current home and away teams in their previous N head-to-head
    encounters against each other. Only non-draw games are considered
    when computing win ratios.

    The computed ratios are added as new columns:
    `h2h_home_win_ratio` and `h2h_away_win_ratio`.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing game-level data. Must include the
        following columns:
        - 'home_team'
        - 'away_team'
        - 'total_home_score'
        - 'total_away_score'
    date_col : str, optional
        Column name used to sort games chronologically within each
        head-to-head matchup. If not provided or not found, the existing
        DataFrame index order is used.
    n : int, default 5
        Number of most recent head-to-head games to consider when
        computing win ratios.

    Returns
    -------
    pd.DataFrame
        A DataFrame with the same number of rows as `df`, augmented with:
        - `h2h_home_win_ratio` : Rolling win ratio of the home team over
          the last N head-to-head games.
        - `h2h_away_win_ratio` : Rolling win ratio of the away team over
          the last N head-to-head games.

    Notes
    -----
    - Head-to-head matchups are identified by unordered team pairs
      (home/away order does not matter).
    - Draws are excluded from both the numerator and denominator when
      calculating win ratios.
    - If fewer than N prior non-draw games exist, all available games
      are used.
    - If no prior non-draw games exist, both ratios are set to 0.0.
    - The DataFrame must be sorted chronologically (explicitly via
      `date_col` or implicitly via index order) for correct results.
    - The function preserves the original row order within each matchup
      group and does not drop any games.

    Examples
    --------
    >>> df = add_last5_h2h_win_ratios(df, date_col="game_date", n=5)
    >>> df[["home_team", "away_team", "h2h_home_win_ratio", "h2h_away_win_ratio"]].head()
    """
    result_df: pd.DataFrame = df.copy()

    # define matchup key
    result_df['matchup'] = result_df.apply(lambda row: tuple(sorted((row['home_team'], row['away_team']))), axis=1)  # type: ignore

    # determine winner
    result_df['winner'] = np.where(result_df['total_home_score'] > result_df['total_away_score'], result_df['home_team'],
                    np.where(result_df['total_home_score'] < result_df['total_away_score'], result_df['away_team'], 'DRAW'))

    # if you have a date column, sort by it; otherwise index order
    if date_col and date_col in result_df.columns:
        result_df = result_df.sort_values(date_col)  # type: ignore
    else:
        result_df = result_df.sort_index()  # type: ignore

    def compute_ratios(group: pd.DataFrame) -> pd.DataFrame:
        home_ratios: List[float] = []
        away_ratios: List[float] = []
        home_team: Optional[str] = None

        for i, row in group.iterrows():
            if home_team is None:
                home_team = row['home_team']
                _ = row['away_team']

            # look at last n games only
            past_games = group.loc[:i-1] if isinstance(group.index, pd.RangeIndex) else group.loc[:i].iloc[:-1]  # type: ignore
            past_games = past_games.tail(n)   # ✅ only last n games

            past_non_draw = past_games[past_games['winner'] != 'DRAW']
            total_games = len(past_non_draw)

            if total_games == 0:
                home_ratios.append(0.0)
                away_ratios.append(0.0)
                continue

            home_wins = (past_non_draw['winner'] == row['home_team']).sum()
            away_wins = (past_non_draw['winner'] == row['away_team']).sum()

            home_ratios.append(home_wins / total_games)
            away_ratios.append(away_wins / total_games)

        group['h2h_home_win_ratio'] = home_ratios
        group['h2h_away_win_ratio'] = away_ratios
        return group

    result_df = result_df.groupby('matchup', group_keys=False).apply(compute_ratios)
    return result_df.drop(columns=['matchup', 'winner'])


def add_historical_win_pct(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add in-season historical win percentages for home and away teams,
    including the current game.

    This function computes cumulative win percentages for each team
    within a season using all games played **up to and including**
    the current game. Win percentages are calculated separately for
    home and away teams and merged back into the original DataFrame.

    Game outcomes are encoded as:
    - Win  = 1.0
    - Loss = 0.0
    - Draw = 0.5

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing game-level data. Must include at
        least the following columns:
        - 'game_date'
        - 'season'
        - 'home_team'
        - 'away_team'
        - 'total_home_score'
        - 'total_away_score'

    Returns
    -------
    pd.DataFrame
        The input DataFrame augmented with two new columns:
        - `home_pct` : Home team’s in-season win percentage after
          the current game.
        - `away_pct` : Away team’s in-season win percentage after
          the current game.

    Notes
    -----
    - Win percentages are computed **within each season** and reset
      at season boundaries.
    - The DataFrame is sorted chronologically by `game_date` before
      computing cumulative statistics.
    - Draws contribute 0.5 wins and count as one game.
    - The function preserves the original row count and ordering
      (aside from temporary internal sorting).
    - The resulting features are suitable for post-game analysis
      and retrospective modeling. For pre-game features, the
      current game should be excluded from the cumulative counts.
    """

    df = df.copy()

    # Ensure datetime
    if not np.issubdtype(df["game_date"].dtype, np.datetime64):
        df["game_date"] = pd.to_datetime(df["game_date"])

    # Sort and create match_id
    df = (
        df.sort_values("game_date")
          .reset_index(drop=False)
          .rename(columns={"index": "match_id"})
    )

    # ---------- Long format ----------
    home = df[[
        "match_id", "game_date", "season",
        "home_team", "total_home_score", "total_away_score"
    ]].copy()
    home.rename(columns={
        "home_team": "team",
        "total_home_score": "points_for",
        "total_away_score": "points_against"
    }, inplace=True)
    home["is_home"] = 1

    away = df[[
        "match_id", "game_date", "season",
        "away_team", "total_away_score", "total_home_score"
    ]].copy()
    away.rename(columns={
        "away_team": "team",
        "total_away_score": "points_for",
        "total_home_score": "points_against"
    }, inplace=True)
    away["is_home"] = 0

    teams_long = pd.concat([home, away], ignore_index=True)

    # Critical sorting
    teams_long = teams_long.sort_values(
        ["team", "season", "game_date", "match_id"]
    )

    # Game result
    teams_long["result"] = np.where(
        teams_long["points_for"] > teams_long["points_against"], 1.0,
        np.where(teams_long["points_for"] < teams_long["points_against"], 0.0, 0.5)
    )

    # INCLUDE current game
    teams_long["cum_wins"] = (
        teams_long.groupby(["team", "season"])["result"].cumsum()
    )
    teams_long["cum_games"] = (
        teams_long.groupby(["team", "season"]).cumcount() + 1
    )

    teams_long["win_pct"] = teams_long["cum_wins"] / teams_long["cum_games"]

    # ---------- Split home / away ----------
    home_pct = (
        teams_long[teams_long["is_home"] == 1]
        [["match_id", "win_pct"]]
        .rename(columns={"win_pct": "home_pct"})
    )

    away_pct = (
        teams_long[teams_long["is_home"] == 0]
        [["match_id", "win_pct"]]
        .rename(columns={"win_pct": "away_pct"})
    )

    # Merge back
    df = df.merge(home_pct, on="match_id", how="left")
    df = df.merge(away_pct, on="match_id", how="left")

    # Cleanup
    df = df.drop(columns=["match_id"])

    return df

def add_home_away_team_avg_scores_before(df):
    """
    Compute season-based average team scores INCLUDING the current game
    for both home and away teams.

    - Averages are computed per season.
    - Home and away teams are treated symmetrically.
    - Includes the current game's score in the average.
    - Safe for post-game analytics, standings, and reporting.
    """

    df = df.copy()
    df = df.reset_index(drop=True)

    home_avg_list = []
    away_avg_list = []

    # {season: {team: [scores]}}
    team_scores = {}

    for _, row in df.iterrows():
        season = row["season"]
        home_team = row["home_team"]
        away_team = row["away_team"]

        # Initialize season dict
        if season not in team_scores:
            team_scores[season] = {}

        # Initialize team lists
        team_scores[season].setdefault(home_team, [])
        team_scores[season].setdefault(away_team, [])

        # ---------------------------
        # UPDATE SCORES FIRST (INCLUDE CURRENT GAME)
        # ---------------------------
        team_scores[season][home_team].append(row["total_home_score"])
        team_scores[season][away_team].append(row["total_away_score"])

        # ---------------------------
        # HOME TEAM AVERAGE (INCLUDING CURRENT)
        # ---------------------------
        home_scores = team_scores[season][home_team]
        home_avg = sum(home_scores) / len(home_scores)
        home_avg_list.append(home_avg)

        # ---------------------------
        # AWAY TEAM AVERAGE (INCLUDING CURRENT)
        # ---------------------------
        away_scores = team_scores[season][away_team]
        away_avg = sum(away_scores) / len(away_scores)
        away_avg_list.append(away_avg)

    df["home_team_avg_score"] = home_avg_list
    df["away_team_avg_score"] = away_avg_list

    return df

def add_league_avg_score_before(df):
    """
    Add season-based average team scores for home and away teams,
    including the current game.

    This function computes per-season average scores for each team
    by accumulating all games played **up to and including** the
    current game. The averages are calculated separately for the
    home and away teams in each match and merged back into the
    original DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing game-level data. Must include
        the following columns:
        - 'season'
        - 'home_team'
        - 'away_team'
        - 'total_home_score'
        - 'total_away_score'

    Returns
    -------
    pd.DataFrame
        The input DataFrame augmented with two new columns:
        - `home_team_avg_score` : Season-to-date average score of
          the home team after the current game.
        - `away_team_avg_score` : Season-to-date average score of
          the away team after the current game.

    Notes
    -----
    - Averages are computed independently for each season and reset
      at season boundaries.
    - The current game’s score is included in the computed averages
      (post-game feature).
    - Home and away teams are treated symmetrically.
    - The function preserves the original row count and ordering.
    - These features are suitable for post-game analysis, standings,
      and reporting. For pre-game modeling, the current game should
      be excluded from the averages.
    """

    df = df.copy()
    df = df.reset_index(drop=True)

    league_avg_list = []

    # {season: [scores]}
    league_scores = {}

    for _, row in df.iterrows():
        season = row["season"]

        if season not in league_scores:
            league_scores[season] = []

        # ---------------------------
        # UPDATE FIRST (INCLUDE CURRENT GAME)
        # ---------------------------
        league_scores[season].append(row["total_home_score"])
        league_scores[season].append(row["total_away_score"])

        # ---------------------------
        # LEAGUE AVG (INCLUDING CURRENT)
        # ---------------------------
        scores = league_scores[season]
        league_avg = sum(scores) / len(scores)
        league_avg_list.append(league_avg)

    # Feature name unchanged
    df["league_avg_score_before"] = league_avg_list

    return df

def add_home_away_team_avg_stat_before(
    df,
    stat_name,
    home_stat_col,
    away_stat_col
):
    """
    Add season-based rolling averages of a team statistic for home and
    away teams, including the current game.

    This function computes per-season average values of a specified
    team-level statistic by accumulating all games played **up to and
    including** the current game. Averages are calculated separately
    for home and away teams and appended to the DataFrame as new features.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing game-level data. Must include:
        - 'season'
        - 'game_date'
        - 'home_team'
        - 'away_team'
        - `home_stat_col`
        - `away_stat_col`
    stat_name : str
        Base name used to construct the output feature columns.
    home_stat_col : str
        Column name containing the home team’s statistic value for
        the current game.
    away_stat_col : str
        Column name containing the away team’s statistic value for
        the current game.

    Returns
    -------
    pd.DataFrame
        The input DataFrame augmented with two new columns:
        - `home_avg_<stat_name>` : Season-to-date average of the
          statistic for the home team after the current game.
        - `away_avg_<stat_name>` : Season-to-date average of the
          statistic for the away team after the current game.

    Notes
    -----
    - Rolling averages are computed independently for each season and
      reset at season boundaries.
    - The current game’s statistic is included in the average
      (post-game feature).
    - Home and away teams are treated symmetrically.
    - Output feature names intentionally do NOT include the suffix
      `_before`, despite the function name.
    - The DataFrame is sorted by season and game date before computing
      averages to ensure chronological correctness.
    - Suitable for post-game analysis, standings, and reporting.
      For pre-game modeling, the current game should be excluded.
    """

    df = df.copy()
    df = df.sort_values(["season", "game_date"]).reset_index(drop=True)

    # {season: {team: [stat_values]}}
    team_stat_history = defaultdict(lambda: defaultdict(list))

    home_avg_list = []
    away_avg_list = []

    for _, row in df.iterrows():
        season = row["season"]
        home_team = row["home_team"]
        away_team = row["away_team"]

        # ---------------------------
        # UPDATE FIRST (INCLUDE CURRENT GAME)
        # ---------------------------
        team_stat_history[season][home_team].append(row[home_stat_col])
        team_stat_history[season][away_team].append(row[away_stat_col])

        # ---------------------------
        # HOME avg (INCLUDING CURRENT)
        # ---------------------------
        home_vals = team_stat_history[season][home_team]
        home_avg_list.append(sum(home_vals) / len(home_vals))

        # ---------------------------
        # AWAY avg (INCLUDING CURRENT)
        # ---------------------------
        away_vals = team_stat_history[season][away_team]
        away_avg_list.append(sum(away_vals) / len(away_vals))

    # ✅ Feature names without `_before`
    df[f"home_avg_{stat_name}"] = home_avg_list
    df[f"away_avg_{stat_name}"] = away_avg_list

    return df


def add_league_avg_stat_before(df, stat_name, home_stat_col, away_stat_col):
    """
    Add season-based league-wide average of a specified statistic
    INCLUDING the current game.

    NOTE:
    - Function name is unchanged.
    - Output column name is unchanged (still contains `_before`).
    - Logic now includes the current game values.
    """

    df = df.copy()
    df = df.sort_values(["season", "game_date"]).reset_index(drop=True)

    # {season: [stat_values]}
    league_stats = defaultdict(list)

    league_avg_list = []

    for _, row in df.iterrows():
        season = row["season"]

        # ---------------------------
        # UPDATE FIRST (INCLUDE CURRENT GAME)
        # ---------------------------
        league_stats[season].append(row[home_stat_col])
        league_stats[season].append(row[away_stat_col])

        # ---------------------------
        # LEAGUE AVG (INCLUDING CURRENT)
        # ---------------------------
        values = league_stats[season]
        league_avg_list.append(sum(values) / len(values))

    # Feature name unchanged
    df[f"league_avg_{stat_name}_before"] = league_avg_list

    return df

def add_pf_pa_by_season(df):
    """
    Add season-based league-wide average of a statistic,
    including the current game.

    This function computes a league-wide average for a specified
    team-level statistic by season. For each game, both the home
    and away team values are added to the season’s league pool
    **before** computing the average, meaning the current game
    is included in the calculation.

    Despite the function name, the resulting feature reflects
    a **post-game** league average.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing game-level data. Must include:
        - 'season'
        - 'game_date'
        - `home_stat_col`
        - `away_stat_col`
    stat_name : str
        Base name used to construct the output feature column.
    home_stat_col : str
        Column name containing the home team’s statistic value.
    away_stat_col : str
        Column name containing the away team’s statistic value.

    Returns
    -------
    pd.DataFrame
        The input DataFrame augmented with one new column:
        - `league_avg_<stat_name>_before` : Season-to-date league
          average of the statistic after the current game.

    Notes
    -----
    - Averages are computed independently for each season.
    - Both home and away team values contribute equally.
    - The current game’s values are included (post-game feature).
    - The output column name intentionally retains the `_before`
      suffix for backward compatibility.
    - The DataFrame is sorted by season and game date prior to
      calculation to ensure chronological correctness.
    """
    df = df.copy().reset_index(drop=True)
    df["df_row"] = df.index  # unique row ID

    # Ensure season exists
    if "season" not in df.columns:
        df["season"] = df["game_date"].dt.year

    # ----- Build long_df -----
    home = pd.DataFrame({
        "team": df["home_team"],
        "PF": df["total_home_score"],
        "PA": df["total_away_score"],
        "game_date": df["game_date"],
        "season": df["season"],
        "df_row": df["df_row"],
        "side": "home"
    })

    away = pd.DataFrame({
        "team": df["away_team"],
        "PF": df["total_away_score"],
        "PA": df["total_home_score"],
        "game_date": df["game_date"],
        "season": df["season"],
        "df_row": df["df_row"],
        "side": "away"
    })

    long_df = pd.concat([home, away], ignore_index=True)

    # Sort correctly for cumulative sum
    long_df = long_df.sort_values(["team", "season", "game_date", "df_row"]).reset_index(drop=True)

    # Team game order WITHIN SEASON
    long_df["team_game_no"] = long_df.groupby(["team", "season"]).cumcount()

    # Season-based cumulative PF & PA
    long_df["cum_PF"] = long_df.groupby(["team", "season"])["PF"].cumsum()
    long_df["cum_PA"] = long_df.groupby(["team", "season"])["PA"].cumsum()

    # Prepare containers
    home_pf = np.zeros(len(df))
    home_pa = np.zeros(len(df))
    away_pf = np.zeros(len(df))
    away_pa = np.zeros(len(df))

    # Assign PF/PA directly (no merges)
    for _, row in long_df.iterrows():
        if row["side"] == "home":
            home_pf[row["df_row"]] = row["cum_PF"]
            home_pa[row["df_row"]] = row["cum_PA"]
        else:
            away_pf[row["df_row"]] = row["cum_PF"]
            away_pa[row["df_row"]] = row["cum_PA"]

    df["home_pf"] = home_pf
    df["home_pa"] = home_pa
    df["away_pf"] = away_pf
    df["away_pa"] = away_pa

    return df
# sync 1774962859229559013
# sys_sync_44e536ff
