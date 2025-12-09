from typing import Dict, Any, List, Tuple, Optional
from sqlite3 import Cursor

def insert_prediction_data(cursor: Cursor, insert_prediction_data_query: str, prediction_data: Dict[str, Any]) -> None:
    """
    Insert NFL game prediction data into the database.

    This function executes a parameterized SQL INSERT statement using the
    provided database cursor to store model prediction results and
    associated game metadata for a single NFL match.

    Parameters
    ----------
    cursor : sqlite3.Cursor
        Database cursor used to execute the SQL INSERT statement.
    insert_prediction_data_query : str
        SQL INSERT query with positional placeholders matching the
        order of values extracted from `prediction_data`.
    prediction_data : dict
        Dictionary containing prediction results and related metadata.
        Expected keys include:

        - game_id : str
            Unique identifier for the NFL game.
        - year : int
            Season year of the game.
        - week : int
            Week number within the NFL season.
        - home_team : str
            Name or abbreviation of the home team.
        - away_team : str
            Name or abbreviation of the away team.
        - home_team_win_probability : float
            Predicted win probability for the home team.
        - away_team_win_probability : float
            Predicted win probability for the away team.
        - predicted_result : str
            Predicted winner of the game.
        - home_team_image_url : str
            URL to the home team's logo or image.
        - away_team_image_url : str
            URL to the away team's logo or image.
        - home_coach : str
            Name of the home team’s head coach.
        - away_coach : str
            Name of the away team’s head coach.
        - stadium : str
            Name of the stadium where the game is played.

    Returns
    -------
    None

    Notes
    -----
    - This function does not commit the transaction; committing must be
      handled by the caller.
    - The SQL query must contain placeholders in the same order as the
      values supplied from `prediction_data`.
    - A `KeyError` will be raised if any required key is missing from
      `prediction_data`.
    """
    _ = cursor.execute(insert_prediction_data_query, (
        prediction_data['game_id'],
        prediction_data['year'],
        prediction_data['week'],
        prediction_data['home_team'],
        prediction_data['away_team'],
        prediction_data['home_team_win_probability'],
        prediction_data['away_team_win_probability'],
        prediction_data['predicted_result'],
        prediction_data['home_team_image_url'],
        prediction_data['away_team_image_url'],
        prediction_data['home_coach'],
        prediction_data['away_coach'],
        prediction_data['stadium']
    ))

def update_actual_result(cursor: Cursor, 
                         update_actual_result_query: str, 
                         game_id: str, 
                         home_score: Optional[int], 
                         away_score: Optional[int]) -> None:
    """
    Update the actual game result in the database after a match concludes.

    This function executes a parameterized SQL UPDATE statement to store
    the final home and away scores for a given NFL game identified by
    `game_id`.

    Parameters
    ----------
    cursor : sqlite3.Cursor
        Database cursor used to execute the SQL UPDATE statement.
    update_actual_result_query : str
        SQL UPDATE query containing positional placeholders for
        `home_score`, `away_score`, and `game_id`.
    game_id : str
        Unique identifier for the NFL game.
    home_score : int or None
        Final score of the home team. Use None if the score is unavailable
        or not yet finalized.
    away_score : int or None
        Final score of the away team. Use None if the score is unavailable
        or not yet finalized.

    Returns
    -------
    None

    Notes
    -----
    - This function does not commit the transaction; committing must be
      handled by the caller.
    - The SQL query must match the parameter order:
      `(home_score, away_score, game_id)`.
    - Passing `None` will result in NULL values in the database, depending
      on the table schema.
    """
    _ = cursor.execute(update_actual_result_query, (home_score, away_score, game_id))

def fetch_predictions(cursor: Cursor, fetch_data_query: str, year: int, week: int) -> List[Tuple[Any, ...]]:
    """
    Retrieve stored NFL game predictions for a specific season and week.

    This function executes a parameterized SQL SELECT query to fetch all
    prediction records corresponding to the given `year` and `week`.

    Parameters
    ----------
    cursor : sqlite3.Cursor
        Database cursor used to execute the SQL SELECT statement.
    fetch_data_query : str
        SQL SELECT query containing positional placeholders for
        `year` and `week`.
    year : int
        NFL season year for which prediction records are requested.
    week : int
        Week number within the NFL season.

    Returns
    -------
    list of tuple
        A list of tuples representing the fetched prediction records.
        Each tuple corresponds to a row returned by the query, with
        column order matching the SELECT statement.

    Notes
    -----
    - This function does not commit or close the database connection.
    - The structure of each returned tuple depends on the columns
      specified in the SELECT query.
    - If no matching records are found, an empty list is returned.
    """
    _ = cursor.execute(fetch_data_query, (year, week))
    return cursor.fetchall()

def fetch_match_scores(cursor: Cursor, fetch_match_scores_query: str, game_id: str) -> List[Tuple[Any, ...]]:
    """
    Retrieve stored match scores for a specific NFL game.

    This function executes a parameterized SQL SELECT query to fetch
    the home and away scores (or related scoring data) for the game
    identified by `game_id`.

    Parameters
    ----------
    cursor : sqlite3.Cursor
        Database cursor used to execute the SQL SELECT statement.
    fetch_match_scores_query : str
        SQL SELECT query containing a positional placeholder for
        `game_id`.
    game_id : str
        Unique identifier for the NFL game.

    Returns
    -------
    list of tuple
        A list of tuples representing the fetched match score records.
        Each tuple corresponds to a row returned by the query, with
        column order matching the SELECT statement.

    Notes
    -----
    - This function does not commit or close the database connection.
    - If no record exists for the given `game_id`, an empty list is returned.
    - The structure of the returned tuples depends on the columns
      specified in the SELECT query.
    """
    _ = cursor.execute(fetch_match_scores_query, (game_id,))
    return cursor.fetchall()

def fetch_prediction_by_game_id(cursor: Cursor, fetch_prediction_by_game_id_query: str, game_id: str) -> List[Tuple[Any, ...]]:
    """
   Retrieve stored prediction data for a specific NFL game.

    This function executes a parameterized SQL SELECT query to fetch
    prediction records associated with the given `game_id`.

    Parameters
    ----------
    cursor : sqlite3.Cursor
        Database cursor used to execute the SQL SELECT statement.
    fetch_prediction_by_game_id_query : str
        SQL SELECT query containing a positional placeholder for
        `game_id`.
    game_id : str
        Unique identifier for the NFL game.

    Returns
    -------
    list of tuple
        A list of tuples representing the fetched prediction records.
        Each tuple corresponds to a row returned by the query, with
        column order matching the SELECT statement.

    Notes
    -----
    - This function does not commit or close the database connection.
    - If no prediction exists for the given `game_id`, an empty list
      is returned.
    - The structure of the returned tuples depends on the columns
      specified in the SELECT query.
    """
    _ = cursor.execute(fetch_prediction_by_game_id_query, (game_id,))
    return cursor.fetchall()

def fetch_all_predictions(cursor: Cursor, fetch_all_predictions_query: str) -> List[Tuple[Any, ...]]:
    """
    Retrieve all stored NFL game prediction records from the database.

    This function executes a SQL SELECT query to fetch every prediction
    record currently stored in the predictions table.

    Parameters
    ----------
    cursor : sqlite3.Cursor
        Database cursor used to execute the SQL SELECT statement.
    fetch_all_predictions_query : str
        SQL SELECT query used to retrieve all prediction records.

    Returns
    -------
    list of tuple
        A list of tuples representing all fetched prediction records.
        Each tuple corresponds to a row returned by the query, with
        column order matching the SELECT statement.

    Notes
    -----
    - This function does not commit or close the database connection.
    - If no prediction records exist, an empty list is returned.
    - The exact contents of each tuple depend on the columns specified
      in the SELECT query (e.g., game_id, year, week, teams, coaches,
      stadium).
    """
    _ = cursor.execute(fetch_all_predictions_query)
    return cursor.fetchall()

def update_probabilities(cursor: Cursor,
                         update_probabilities_query: str,
                         game_id: str,
                         home_prob: float,
                         away_prob: float,
                         predicted_result: str,
                         home_team: str,
                         away_team: str,
                         home_coach: str,
                         away_coach: str,
                         stadium: str,
                         home_team_image_url: str,
                         away_team_image_url: str) -> None:
    """
    Update win probabilities and team details for an NFL game.

    This function executes a parameterized SQL UPDATE statement to modify
    the stored home and away win probabilities, team names, coaches, stadium,
    and predicted winner for the game identified by `game_id`.

    Parameters
    ----------
    cursor : sqlite3.Cursor
        Database cursor used to execute the SQL UPDATE statement.
    update_probabilities_query : str
        SQL UPDATE query containing positional placeholders.
    game_id : str
        Unique identifier for the NFL game.
    home_prob : float
        Updated predicted win probability for the home team.
    away_prob : float
        Updated predicted win probability for the away team.
    predicted_result : str
        Updated predicted winner of the game.
    home_team : str
        Updated home team name.
    away_team : str
        Updated away team name.
    home_coach : str
        Updated home coach name.
    away_coach : str
        Updated away coach name.
    stadium : str
        Updated stadium name.
    home_team_image_url : str
        Updated home team logo URL.
    away_team_image_url : str
        Updated away team logo URL.

    Returns
    -------
    None
    """
    _ = cursor.execute(update_probabilities_query, (
        home_prob, 
        away_prob, 
        predicted_result, 
        home_team, 
        away_team, 
        home_coach, 
        away_coach, 
        stadium, 
        home_team_image_url, 
        away_team_image_url, 
        game_id
    ))
# sys_sync_66fe1f77
# sys_sync_ed3300
# sys_sync_71231f28
# sys_sync_2a9f1062
# sys_sync_8532c8f
