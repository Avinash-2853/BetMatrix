import requests
from functools import lru_cache
from typing import Optional, Tuple, List, Dict, Any
from app.core.constant import (
    ERROR_FETCHING_GAME_DATE,
    ERROR_PARSING_GAME_DATE,
    UNEXPECTED_ERROR_GAME_DATE,
    ERROR_COMPETITORS_INCOMPLETE,
    ERROR_FETCHING_SCORE,
    ERROR_CANNOT_DETERMINE_SCORES,
    ERROR_FETCHING_DATA_ESPN,
    ERROR_PARSING_JSON_DATA,
    UNEXPECTED_ERROR_GET_DATA
)

@lru_cache(maxsize=32)
def get_coach_name(team_id: str, year: int) -> Tuple[Optional[str], Optional[str]]:
    """
    Retrieve the head coach’s name for a given NFL team and season.

    This function queries the ESPN public API to fetch the head coach
    information associated with a specific team and season. It returns
    the coach’s first and last name if available.

    Parameters
    ----------
    team_id : str
        ESPN team identifier for the NFL team.
    year : int
        NFL season year for which the head coach information is requested.

    Returns
    -------
    tuple of (str or None, str or None)
        A tuple containing the coach’s first name and last name.
        Returns `(None, None)` if the coach information cannot be
        retrieved or does not exist.

    Notes
    -----
    - This function depends on the ESPN public API and requires
      network connectivity.
    - The API response structure may change over time.
    - Only the first listed coach entry is considered.
    - Errors due to missing data, malformed responses, or request
      failures are safely handled and result in `(None, None)`.
    """
    url = f"https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{year}/teams/{team_id}/coaches"
    
    try:
        resp = requests.get(url, timeout=10).json()
        coach_details = requests.get(resp["items"][0]["$ref"], timeout=10).json()
        return coach_details.get("firstName"), coach_details.get("lastName")
    except Exception:
        return None, None


def get_team_logo(team_id: str, year: int) -> Optional[str]:
    """
    Retrieve the logo URL for an NFL team for a given season.

    This function queries the ESPN public API to fetch metadata for a
    specific NFL team and returns the URL of the team’s primary logo
    if available.

    Parameters
    ----------
    team_id : str
        ESPN team identifier for the NFL team.
    year : int
        NFL season year for which the team logo is requested.

    Returns
    -------
    str or None
        URL of the team’s logo image if found; otherwise, None.

    Notes
    -----
    - This function depends on the ESPN public API and requires
      network connectivity.
    - Only the first logo entry in the API response is used.
    - The API response format may change over time.
    - Request failures or missing logo data are safely handled and
      result in `None`.
    """
    try:
        url = f"https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{year}/teams/{team_id}?lang=en&region=us"
        response = requests.get(url, timeout=10)
        data = response.json()

        logos = data.get("logos", [])
        if logos and isinstance(logos, list):
            return logos[0].get("href")  # type: ignore
    except requests.RequestException:
        pass

    return None


def map_team(team_name: str) -> str:
    """ 
    Normalize NFL team abbreviations to standard forms.

    This function maps specific team abbreviations to their standardized
    equivalents to ensure consistency across datasets.

    Parameters
    ----------
    team_name : str
        Team abbreviation to normalize.

    Returns
    -------
    str
        Standardized team abbreviation.

    Notes
    -----
    - Currently applies the following mappings:
      - 'WSH' → 'WAS'
      - 'LAR' → 'LA'
    - All other team abbreviations are returned unchanged.
    """
    if team_name == "WSH":
        return "WAS"
    if team_name == "LAR":
        return "LA"
    return team_name


def get_team_details(year: int, week: int) -> List[Dict[str, Any]]:
    """
    Retrieve NFL game details for a specific season week.

    This function queries the ESPN public schedule API to collect
    metadata for all NFL games in a given week, including team
    identifiers, coaching information, venue, and branding assets.

    For each scheduled game, the function returns standardized home
    and away team abbreviations, head coach names, stadium name,
    unique game ID, and team logo URLs.

    Parameters
    ----------
    year : int
        NFL season year for which the schedule data is requested.
    week : int
        Week number of the NFL season.

    Returns
    -------
    list of dict
        A list of dictionaries, one per game, each containing:
        - `game_id` : str
            Unique identifier for the game.
        - `home_team` : str
            Standardized home team abbreviation.
        - `away_team` : str
            Standardized away team abbreviation.
        - `home_coach` : str
            Full name of the home team’s head coach.
        - `away_coach` : str
            Full name of the away team’s head coach.
        - `home_team_logo_url` : str or None
            URL to the home team’s logo image.
        - `away_team_logo_url` : str or None
            URL to the away team’s logo image.
        - `stadium` : str or None
            Name of the stadium where the game is played.

    Notes
    -----
    - This function depends on ESPN public APIs and requires network
      connectivity.
    - Team abbreviations are normalized using `map_team`.
    - Coach names are fetched using `get_coach_name`.
    - Team logos are fetched using `get_team_logo`.
    - API response structures may change over time.
    - Games with missing home or away team data are skipped.
    """
    if week > 18:
        # Playoffs: Match Week 19 -> Week 1, etc.
        # Season Type 3 = Postseason
        query_week = week - 18
        season_type = 3
    else:
        # Regular Season
        query_week = week
        season_type = 2

    url = f"https://cdn.espn.com/core/nfl/schedule?xhr=1&year={year}&week={query_week}&seasontype={season_type}"
    try:
        response = requests.get(url, timeout=30)
        data = response.json()
    except Exception as e:
        print(f"Error fetching schedule: {e}")
        return []

    games: List[Dict[str, Any]] = []
    schedule = data.get("content", {}).get("schedule", {})

    for _date_key, date_data in schedule.items():
        games_list = date_data.get("games", [])
        print(f"Found {len(games_list)} games for date {_date_key}")
        for game in games_list:
            comp = game["competitions"][0]
            teams = comp["competitors"]

            home = next((t for t in teams if t.get("homeAway") == "home"), None)
            away = next((t for t in teams if t.get("homeAway") == "away"), None)
            if not home or not away:
                print(f"Skipping game {comp.get('id')}: missing home/away competitors")
                continue

            home_abbr = home["team"].get("abbreviation")
            away_abbr = away["team"].get("abbreviation")
            stadium = comp.get("venue", {}).get("fullName")
            game_id = comp.get("id")

            home_id = home["team"].get("id")
            away_id = away["team"].get("id")

            home_logo = get_team_logo(home_id, year)
            away_logo = get_team_logo(away_id, year)

            home_first, home_last = get_coach_name(home_id, year)
            away_first, away_last = get_coach_name(away_id, year)

            games.append({
                "game_id": game_id,
                "home_team": map_team(home_abbr),
                "home_coach": f"{home_first or ''} {home_last or ''}".strip(),
                "home_team_logo_url": home_logo,
                "away_team": map_team(away_abbr),
                "away_coach": f"{away_first or ''} {away_last or ''}".strip(),
                "away_team_logo_url": away_logo,
                "stadium": stadium
            })

    return games


def get_game_date(event_id: str) -> Optional[str]:
    """
    Retrieve the game date for an NFL event from ESPN's public API.

    This function queries the ESPN event API using a unique event ID
    and extracts the scheduled game date. The date is returned in
    ISO format (`YYYY-MM-DD`) when possible.

    Parameters
    ----------
    event_id : str
        Unique identifier for the ESPN NFL event (e.g., "401220116").

    Returns
    -------
    str or None
        Game date formatted as `YYYY-MM-DD` if successfully retrieved
        and parsed; otherwise, None.

    Notes
    -----
    - The function attempts to parse the date from
      `competitions[0].date` in the API response.
    - ISO 8601 timestamps ending with `Z` are handled correctly.
    - If parsing fails, a fallback substring (`YYYY-MM-DD`) may be
      returned when available.
    - Network errors, malformed responses, or missing data are
      handled gracefully and result in `None`.
    - Error details may be logged or printed depending on the
      surrounding application configuration.
    """
    base_url = f"https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/{event_id}"
    
    try:
        response = requests.get(base_url, timeout=10)
        response.raise_for_status()
        event_data = response.json()
        
        # Get the date from competitions[0].date
        competitions = event_data.get("competitions", [])
        if competitions and len(competitions) > 0:
            date_str = competitions[0].get("date")
            if date_str:
                # Parse ISO format date and return YYYY-MM-DD
                from datetime import datetime
                try:
                    dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    return dt.strftime("%Y-%m-%d")
                except (ValueError, AttributeError):
                    # Try parsing other formats if needed
                    return date_str[:10] if len(date_str) >= 10 else None
        return None
        
    except requests.exceptions.RequestException as e:
        print(ERROR_FETCHING_GAME_DATE.format(event_id, e))
        return None
    except (KeyError, IndexError, TypeError) as e:
        print(ERROR_PARSING_GAME_DATE.format(event_id, e))
        return None
    except Exception as e:
        print(UNEXPECTED_ERROR_GAME_DATE.format(event_id, e))
        return None


def _fetch_single_score(score_ref_url: str, event_id: str, team_side: str) -> Optional[int]:
    """
    Retrieve a single team’s score from an ESPN API score endpoint.

    This helper function queries a score reference URL provided by the
    ESPN API and extracts the numeric score value for a single competitor
    (home or away). It is intended for internal use when assembling
    final game score data.

    Parameters
    ----------
    score_ref_url : str
        ESPN API endpoint URL that contains the score information.
    event_id : str
        Unique identifier of the event, used for error logging.
    team_side : str
        Team side identifier used for logging purposes
        (e.g., "home" or "away").

    Returns
    -------
    int or None
        The team’s score as an integer if available; otherwise, None.

    Notes
    -----
    - The function does not default missing scores to zero.
    - Network errors, invalid responses, or missing score values are
      handled gracefully and result in `None`.
    - Intended for internal use (prefixed with an underscore).
    """
    try:
        score_response = requests.get(score_ref_url, timeout=10)
        score_response.raise_for_status()
        score_data = score_response.json()
        score_value_raw = score_data.get("value")
        
        # Only set score if value exists and is not None (don't default to 0)
        if score_value_raw is not None:
            return int(score_value_raw)
        return None
    except (requests.exceptions.RequestException, ValueError, TypeError) as e:
        print(ERROR_FETCHING_SCORE.format(event_id, team_side, e))
        return None


def _process_competitors_for_scores(competitors: List[Dict], event_id: str) -> Dict[str, int]:
    """
    Extract home and away scores from ESPN competitor data.

    This helper function processes a list of competitor objects returned
    by the ESPN API and retrieves the final scores for the home and away
    teams. Each competitor’s score is fetched via its referenced score
    endpoint.

    Parameters
    ----------
    competitors : list of dict
        List of competitor dictionaries from the ESPN API response.
        Each dictionary is expected to contain:
        - 'homeAway' : str ("home" or "away")
        - 'score' : dict with a '$ref' URL pointing to the score endpoint
    event_id : str
        Unique event identifier used for error logging.

    Returns
    -------
    dict
        Dictionary containing extracted scores with the following keys
        when available:
        - 'home_score' : int
        - 'away_score' : int

        The dictionary may be incomplete if one or both scores could
        not be retrieved.

    Notes
    -----
    - Scores are fetched via `_fetch_single_score`.
    - Missing or malformed competitor entries are skipped.
    - The function does not assume a fixed order of competitors.
    - Intended for internal use (prefixed with an underscore).
    """
    scores: Dict[str, int] = {}
    
    # Loop through the two competitors to get their scores.
    for competitor in competitors:
        team_side = competitor.get("homeAway")
        
        # The 'score' key contains a dictionary with a '$ref' URL to the actual score data.
        # We must fetch this URL to get the score value.
        score_ref_url = competitor.get("score", {}).get("$ref")
        
        if not team_side or not score_ref_url:
            continue  # Skip if essential data is missing

        # Fetch the score from its specific endpoint and get the 'value'.
        score_value = _fetch_single_score(score_ref_url, event_id, team_side)
        
        if score_value is not None:
            if team_side == "home":
                scores["home_score"] = score_value
            elif team_side == "away":
                scores["away_score"] = score_value
    
    return scores


def get_match_scores(event_id: str) -> Optional[Dict[str, int]]:
    """
   Retrieve final home and away scores for an NFL game from ESPN's API.

    This function queries the ESPN event API for a given NFL event and
    extracts the final scores for both home and away teams by following
    the provided score reference (`$ref`) endpoints. It is designed to
    safely retrieve completed game results without defaulting missing
    values to zero.

    Parameters
    ----------
    event_id : str
        Unique identifier for the ESPN NFL event (e.g., "401220116").

    Returns
    -------
    dict or None
        Dictionary containing final scores with the following keys:
        - `home_score` : int
        - `away_score` : int

        Returns `None` if scores cannot be fully retrieved or if an
        error occurs.

    Notes
    -----
    - This function should be called **only for games that have already
      been played**. Use `get_game_date()` to validate game completion.
    - Scores are retrieved via `_process_competitors_for_scores` and
      `_fetch_single_score`.
    - Partial score data is not returned; both scores must be available
      or the function returns `None`.
    - Missing or unavailable scores are never defaulted to 0.
    - Network errors, malformed API responses, or unexpected data
      structures are handled gracefully.

    """
    # The primary URL for the event data
    base_url = f"https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/{event_id}"
    
    try:
        # Step 1: Fetch the main event data in a single request.
        response = requests.get(base_url, timeout=10)
        response.raise_for_status()
        event_data = response.json()

        # Step 2: Navigate directly to the list of competitors.
        # The structure is event -> competitions[0] -> competitors
        competitors = event_data.get("competitions", [{}])[0].get("competitors", [])

        if not competitors or len(competitors) != 2:
            print(ERROR_COMPETITORS_INCOMPLETE.format(event_id))
            return None

        # Step 3: Process competitors to extract scores
        scores = _process_competitors_for_scores(competitors, event_id)
        
        # Ensure both scores were found before returning
        # If either score is missing, return None (do NOT return partial scores)
        if "home_score" in scores and "away_score" in scores:
            return scores
        
        print(ERROR_CANNOT_DETERMINE_SCORES.format(event_id))
        return None

    except requests.exceptions.RequestException as e:
        print(ERROR_FETCHING_DATA_ESPN.format(event_id, e))
        return None
    except (KeyError, IndexError, TypeError) as e:
        print(ERROR_PARSING_JSON_DATA.format(event_id, e))
        return None
    except Exception as e:
        print(UNEXPECTED_ERROR_GET_DATA.format(event_id, e))
        return None
# sync 1774962859580044249
# sys_sync_14002d14
# sys_sync_c2a2edc
# sys_sync_63e1390b
