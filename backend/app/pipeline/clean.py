import pandas as pd
import numpy as np
import sys, os
from typing import List, Optional
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from app.core.constant import provide_column_list_message


def remove_columns(df: pd.DataFrame, column_list: List[str]) -> pd.DataFrame:
    """
    Remove specified columns from a DataFrame.

    Parameters
    ----------
    df : pandas.DataFrame
        The input DataFrame from which columns need to be removed.
    column_list : list
        A list of column names to drop from the DataFrame.

    Returns
    -------
    pandas.DataFrame
        A new DataFrame with the specified columns removed.

    Notes
    -----
    - Raises a KeyError if any of the specified columns are not present in `df`.
    - Does not modify the original DataFrame; instead, returns a copy without the columns.
    """

    # Drop the specified columns from the DataFrame
    df1 = df.drop(columns=column_list)

    # Return the updated DataFrame
    return df1


def aggregate_categorical_counts(
    df: pd.DataFrame,
    cat_cols: List[str],
    game_id_col: str = "game_id",
    posteam_col: str = "posteam",
    home_team_col: str = "home_team",
    away_team_col: str = "away_team",
    time_col: str = "game_seconds_remaining"
) -> pd.DataFrame:
    """
    Aggregate per-game categorical event counts for home and away teams
    and append them to the original DataFrame without reducing its size.

    For each categorical column in `cat_cols`, this function:
    - Counts occurrences of each category per team (`posteam`) per game.
    - Creates separate feature columns for home and away teams
      (e.g., `home_event_run`, `away_event_pass`).
    - Merges these aggregated counts back into the original DataFrame.

    To avoid data leakage across play-by-play rows, the aggregated
    categorical counts are retained **only in the final row of each game**.
    All earlier rows within the same game will have these aggregated
    columns set to NaN.

    The final row of a game is identified as a row where:
    - `time_col == 0`, or
    - `time_col` is NaN.

    Parameters
    ----------
    df : pd.DataFrame
        Play-by-play or event-level DataFrame containing multiple rows per game.
    cat_cols : List[str]
        List of categorical columns to aggregate (e.g., play type, event type).
    game_id_col : str, default "game_id"
        Column identifying each unique game.
    posteam_col : str, default "posteam"
        Column indicating the team responsible for the event.
    home_team_col : str, default "home_team"
        Column containing the home team identifier for each game.
    away_team_col : str, default "away_team"
        Column containing the away team identifier for each game.
    time_col : str, default "game_seconds_remaining"
        Column used to determine the final row of each game.

    Returns
    -------
    pd.DataFrame
        A DataFrame with the same number of rows as `df`, augmented with
        aggregated categorical count features for home and away teams.
        Aggregated values are populated only in the final row of each game;
        all earlier rows contain NaN for these features.

    Notes
    -----
    - The function does not modify the row order or drop any rows.
    - Feature column names follow the pattern:
      `{home|away}_{categorical_column}_{category_value}`.
    - This design is suitable for post-game modeling or supervised learning
      setups where only final-game aggregates are required.
    """

    result_df = df.copy()

    for col in cat_cols:
        # Get counts per team per game
        counts = (
            df.groupby([game_id_col, posteam_col])[col]  # type: ignore
            .value_counts()
            .unstack(fill_value=0)
            .reset_index()
        )
        counts = counts.rename(
            columns={c: f"{col}_{c}" for c in counts.columns if c not in [game_id_col, posteam_col]}
        )

        # Home team counts
        home_counts = counts.add_prefix("home_")
        result_df = result_df.merge(  # type: ignore
            home_counts,
            left_on=[game_id_col, home_team_col],
            right_on=[f"home_{game_id_col}", f"home_{posteam_col}"],
            how="left"
        ).drop(columns=[f"home_{game_id_col}", f"home_{posteam_col}"])

        # Away team counts
        away_counts = counts.add_prefix("away_")
        result_df = result_df.merge(  # type: ignore
            away_counts,
            left_on=[game_id_col, away_team_col],
            right_on=[f"away_{game_id_col}", f"away_{posteam_col}"],
            how="left"
        ).drop(columns=[f"away_{game_id_col}", f"away_{posteam_col}"])

    # --- Keep counts only at the last row of each game ---
    def mask_non_last(group: pd.DataFrame) -> pd.DataFrame:
        # all but the last row -> set new cols to NaN
        new_cols: List[str] = [c for c in group.columns if any(cc in c for cc in cat_cols)]
        mask = ~((group[time_col] == 0) | (group[time_col].isna()))
        group.loc[mask, new_cols] = pd.NA
        return group

    result_df = result_df.groupby(game_id_col, group_keys=False).apply(mask_non_last)  # type: ignore

    return result_df


def aggregate_match_features_with_nulls(
    df: pd.DataFrame,
    game_id_col: str = "game_id",
    home_team_col: str = "home_team",
    away_team_col: str = "away_team",
    posteam_col: str = "posteam",
    last_play_col: str = "game_seconds_remaining",
    stat_cols: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Aggregate play-by-play statistics into match-level features for
    home and away teams while preserving the original DataFrame shape.

    This function computes per-game, per-team aggregated statistics
    (using sum) for the specified `stat_cols` and merges them back
    into the play-by-play DataFrame as home and away features.
    The aggregated values are populated **only in the final row
    of each game** and set to NaN for all other rows to prevent
    information leakage across plays.

    A row is considered the final play of a game when
    `last_play_col == 0`.

    Parameters
    ----------
    df : pd.DataFrame
        Play-by-play or event-level DataFrame containing multiple
        rows per game.
    game_id_col : str, default "game_id"
        Column identifying each unique game.
    home_team_col : str, default "home_team"
        Column containing the home team identifier.
    away_team_col : str, default "away_team"
        Column containing the away team identifier.
    posteam_col : str, default "posteam"
        Column indicating the team responsible for each play.
    last_play_col : str, default "game_seconds_remaining"
        Column used to identify the final play of each game.
    stat_cols : list of str
        List of numeric columns to aggregate using sum.
        Must be explicitly provided.

    Returns
    -------
    pd.DataFrame
        A DataFrame with the same number of rows as `df`, augmented
        with aggregated match-level features for home and away teams.
        Aggregated values are present only in the final row of each
        game and NaN elsewhere.

    Raises
    ------
    ValueError
        If `stat_cols` is not provided.

    Notes
    -----
    - The function does not drop, reorder, or duplicate rows.
    - Aggregated feature names follow the pattern:
      `home_<stat>` and `away_<stat>`.
    - This design is suitable for post-game analysis, supervised
      learning, and leakage-safe feature engineering.
    """
    
    if stat_cols is None:
        raise ValueError(provide_column_list_message)
    
    # Ensure df is a pandas DataFrame with copy method
    import pandas as pd
    if not isinstance(df, pd.DataFrame):
        df = pd.DataFrame(df)
    df = df.copy()
    
    # 1. Aggregate by game_id and posteam
    team_stats = (
        df.groupby([game_id_col, posteam_col], as_index=False)[stat_cols].sum()  # type: ignore
    )
    
    # 2. Merge home stats
    df = df.merge(  # type: ignore
        team_stats.add_prefix("home_"),
        left_on=[game_id_col, home_team_col],
        right_on=[f"home_{game_id_col}", f"home_{posteam_col}"],
        how="left"
    )
    
    # 3. Merge away stats
    df = df.merge(  # type: ignore
        team_stats.add_prefix("away_"),
        left_on=[game_id_col, away_team_col],
        right_on=[f"away_{game_id_col}", f"away_{posteam_col}"],
        how="left"
    )
    
    # 4. Drop duplicate key cols created by merge
    drop_cols = [f"home_{game_id_col}", f"home_{posteam_col}", 
                 f"away_{game_id_col}", f"away_{posteam_col}"]
    df.drop(columns=drop_cols, inplace=True, errors="ignore")
    
    # 5. Mask out rows where it's not the last play of the game
    agg_cols = [f"home_{c}" for c in stat_cols] + [f"away_{c}" for c in stat_cols]
    for col in agg_cols:
        df[col] = df[col].where(df[last_play_col] == 0, np.nan)  # type: ignore
    
    return df


def aggregate_positive_negative(
    df: pd.DataFrame,
    num_cols: List[str],
    game_id_col: str = "game_id",
    posteam_col: str = "posteam",
    home_team_col: str = "home_team",
    away_team_col: str = "away_team",
    time_col: str = "game_seconds_remaining"
) -> pd.DataFrame:
    """
    Aggregate positive and negative components of numeric play-by-play
    statistics into match-level features for home and away teams.

    For each numeric column in `num_cols`, the function separates values
    into positive (> 0) and negative (< 0) components, aggregates them
    by team and game using summation, and merges the results back into
    the original DataFrame as home and away features.

    To prevent information leakage across plays, the aggregated features
    are retained **only in the final row of each game** and set to NaN
    for all earlier rows within the same game.

    The final row of a game is identified as a row where:
    - `time_col == 0`, or
    - `time_col` is NaN.

    Parameters
    ----------
    df : pd.DataFrame
        Play-by-play or event-level DataFrame containing multiple rows
        per game.
    num_cols : list of str
        List of numeric columns whose positive and negative components
        will be aggregated.
    game_id_col : str, default "game_id"
        Column identifying each unique game.
    posteam_col : str, default "posteam"
        Column indicating the team responsible for each play.
    home_team_col : str, default "home_team"
        Column containing the home team identifier.
    away_team_col : str, default "away_team"
        Column containing the away team identifier.
    time_col : str, default "game_seconds_remaining"
        Column used to identify the final play of each game.

    Returns
    -------
    pd.DataFrame
        A DataFrame with the same number of rows as `df`, augmented with
        aggregated positive and negative features for home and away teams.
        Aggregated values are present only in the final row of each game
        and NaN elsewhere.

    Notes
    -----
    - Feature column naming convention:
      `home_<stat>_positive`, `home_<stat>_negative`,
      `away_<stat>_positive`, `away_<stat>_negative`.
    - The function preserves row order and does not drop or duplicate rows.
    - Suitable for feature engineering where directional impact
      (gains vs losses) is meaningful.
    """
    result_df = df.copy()

    for col in num_cols:
        # Split into positive and negative parts
        df[f"{col}_positive"] = df[col].where(df[col] > 0, 0)  # type: ignore
        df[f"{col}_negative"] = df[col].where(df[col] < 0, 0)  # type: ignore

        for sign in ["positive", "negative"]:
            stat_col = f"{col}_{sign}"

            # Aggregate by team and game
            agg = (
                df.groupby([game_id_col, posteam_col])[stat_col]  # type: ignore
                .sum()
                .reset_index()
            )

            # Home
            home_agg = agg.rename(
                columns={
                    posteam_col: home_team_col,
                    stat_col: f"home_{stat_col}"
                }
            )
            result_df = result_df.merge(  # type: ignore
                home_agg,
                on=[game_id_col, home_team_col],
                how="left"
            )

            # Away
            away_agg = agg.rename(
                columns={
                    posteam_col: away_team_col,
                    stat_col: f"away_{stat_col}"
                }
            )
            result_df = result_df.merge(  # type: ignore
                away_agg,
                on=[game_id_col, away_team_col],
                how="left"
            )

    # --- Keep only last row per game ---
    def mask_non_last(group: pd.DataFrame) -> pd.DataFrame:
        new_cols: List[str] = [c for c in group.columns if any(f"{col}_" in c for col in num_cols)]
        mask = ~((group[time_col] == 0) | (group[time_col].isna()))
        group.loc[mask, new_cols] = pd.NA
        return group

    result_df = result_df.groupby(game_id_col, group_keys=False).apply(mask_non_last)  # type: ignore

    return result_df
# sys_sync_56905b0b
# sys_sync_2cfa60d2
