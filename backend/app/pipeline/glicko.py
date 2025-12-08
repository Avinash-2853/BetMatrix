# type: ignore
import pandas as pd
import math
from typing import Dict, List

# -------------------------------
# Glicko2 Player Class
# -------------------------------
class Glicko2Player:
    """
    Represents a single player/team in the Glicko-2 rating system.

    Internally stores ratings in the Glicko-2 scale (mu, phi, sigma),
    while exposing helper methods to retrieve values in the
    conventional rating scale.

    Parameters
    ----------
    rating : float, default 1500
        Initial rating on the standard Glicko scale.
    rd : float, default 350
        Initial rating deviation (uncertainty).
    vol : float, default 0.06
        Initial rating volatility.

    Notes
    -----
    - Internally converts ratings using the Glicko-2 transformation:
      mu = (rating - 1500) / 173.7178
      phi = rd / 173.7178
    - Designed to represent teams (not individuals) in sports analytics.
    """
    def __init__(self, rating: float = 1500, rd: float = 350, vol: float = 0.06) -> None:
        # Convert rating scale to Glicko-2 scale
        self.mu = (rating - 1500) / 173.7178
        self.phi = rd / 173.7178
        self.sigma = vol

    def get_rating(self) -> float:
        return self.mu * 173.7178 + 1500

    def get_rd(self) -> float:
        return self.phi * 173.7178

    def get_vol(self) -> float:
        return self.sigma



# -------------------------------
# Glicko2 System
# -------------------------------
class Glicko2:
    """
    Glicko-2 rating system manager for multiple players or teams.

    This class maintains a collection of `Glicko2Player` objects and
    provides methods to update ratings based on match outcomes using
    the official Glicko-2 algorithm.

    Parameters
    ----------
    tau : float, default 0.5
        System constant controlling volatility change.
        Smaller values constrain volatility updates.

    Attributes
    ----------
    players : dict[str, Glicko2Player]
        Mapping of player/team identifiers to their Glicko-2 state.

    Notes
    -----
    - Implementation follows the original Glicko-2 paper by Mark Glickman.
    - Ratings are updated sequentially per match.
    - Suitable for Elo-style team rating in leagues and seasons.
    """
    def __init__(self, tau: float = 0.5) -> None:
        self.tau = tau
        self.players: Dict[str, Glicko2Player] = {}

    def ensure_player(self, name: str) -> Glicko2Player:
        if name not in self.players:
            self.players[name] = Glicko2Player()
        return self.players[name]

    def _g(self, phi: float) -> float:
        return 1 / math.sqrt(1 + 3 * (phi ** 2) / (math.pi ** 2))

    def _expected_score(self, mu: float, mu_j: float, phi_j: float) -> float:
        return 1 / (1 + math.exp(-self._g(phi_j) * (mu - mu_j)))

    def _update_sigma(self, player: Glicko2Player, delta: float, v: float) -> float:
        # Follows Glicko2 paper (step 5)
        a: float = math.log(player.sigma ** 2)
        tau: float = self.tau
        eps: float = 1e-6

        def f(x: float) -> float:
            exp_x: float = math.exp(x)
            num: float = exp_x * (delta**2 - player.phi**2 - v - exp_x)
            den: float = 2 * (player.phi**2 + v + exp_x) ** 2
            return (num / den) - ((x - a) / (tau**2))

        # Step 5.2: set initial bounds
        if delta**2 > (player.phi**2 + v):
            b_val: float = math.log(delta**2 - player.phi**2 - v)
        else:
            k: int = 1
            while f(a - k * tau) < 0:
                k += 1
            b_val = a - k * tau

        A: float = a
        fa: float = f(A)
        fb: float = f(b_val)

        # Step 5.3: iterate until convergence
        while abs(b_val - A) > eps:
            C: float = A + (A - b_val) * fa / (fb - fa)
            fc: float = f(C)
            if fc * fb < 0:
                A, fa = b_val, fb
            else:
                fa /= 2
            b_val, fb = C, fc

        return math.exp(A / 2)

    def update_ratings(self, player_name: str, opponents: List[str], scores: List[float]) -> None:
        """
        Update a player's Glicko-2 rating based on match results.

        Parameters
        ----------
        player_name : str
            Name/identifier of the player or team being updated.
        opponents : list of str
            List of opponent identifiers faced in the rating period.
        scores : list of float
            Match results corresponding to opponents:
            - 1.0 = win
            - 0.5 = draw
            - 0.0 = loss

        Returns
        -------
        None

        Notes
        -----
        - Supports multiple opponents in a single rating period.
        - If no valid games are present, the rating is not updated.
        - Volatility (`sigma`), rating deviation (`phi`), and rating (`mu`)
          are updated in accordance with Glicko-2 steps 5–8.
        """
        player: Glicko2Player = self.players[player_name]

        v_inv: float = 0
        delta_sum: float = 0
        for opp_name, score in zip(opponents, scores):
            opp: Glicko2Player = self.players[opp_name]
            E: float = self._expected_score(player.mu, opp.mu, opp.phi)
            g: float = self._g(opp.phi)
            v_inv += (g ** 2) * E * (1 - E)
            delta_sum += g * (score - E)

        if v_inv == 0:  # no games
            return

        v: float = 1 / v_inv
        delta: float = v * delta_sum

        # update volatility
        sigma_new: float = self._update_sigma(player, delta, v)

        # step 6: update phi*
        phi_star: float = math.sqrt(player.phi ** 2 + sigma_new ** 2)

        # step 7: new phi
        phi_new: float = 1 / math.sqrt((1 / phi_star ** 2) + (1 / v))

        # step 8: new mu
        mu_new: float = player.mu + phi_new ** 2 * delta_sum

        # assign updated values
        player.mu = mu_new
        player.phi = phi_new
        player.sigma = sigma_new

    def get_player(self, name: str) -> Glicko2Player:
        return self.players[name]


# -------------------------------
# Function to add Glicko features
# -------------------------------
def add_glicko_features(df: pd.DataFrame, home_col: str = "home_team", away_col: str = "away_team",
                        home_score_col: str = "total_home_score", away_score_col: str = "total_away_score") -> pd.DataFrame:
    """
    Add pre-game Glicko-2 rating features for home and away teams.

    This function iterates through games in chronological order,
    computes Glicko-2 ratings for each team, and appends the
    **pre-match** rating information as model features.

    Ratings are updated **after** each game, ensuring that the
    added features are leakage-safe and suitable for predictive modeling.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing match-level data.
    home_col : str, default "home_team"
        Column name identifying the home team.
    away_col : str, default "away_team"
        Column name identifying the away team.
    home_score_col : str, default "total_home_score"
        Column containing the home team’s final score.
    away_score_col : str, default "total_away_score"
        Column containing the away team’s final score.

    Returns
    -------
    pd.DataFrame
        DataFrame augmented with the following columns:
        - `home_team_glicko_rating`
        - `home_team_rd`
        - `home_team_vol`
        - `away_team_glicko_rating`
        - `away_team_rd`
        - `away_team_vol`

    Notes
    -----
    - Ratings are captured **before** the match outcome is applied.
    - Match results are encoded as:
        win = 1.0, loss = 0.0, draw = 0.5
    - The DataFrame must be sorted chronologically before calling
      this function for correct rating evolution.
    - Designed for sports outcome prediction and ranking analysis.
    """

    glicko: Glicko2 = Glicko2()
    ratings_data: List[Dict[str, float]] = []

    for _, row in df.iterrows():
        home: Glicko2Player = glicko.ensure_player(row[home_col])
        away: Glicko2Player = glicko.ensure_player(row[away_col])

        if row[home_score_col] > row[away_score_col]:
            home_score: float = 1.0
            away_score: float = 0.0
        elif row[home_score_col] < row[away_score_col]:
            home_score = 0.0
            away_score = 1.0
        else:
            home_score = 0.5
            away_score = 0.5

        # record ratings BEFORE the match
        ratings_data.append({
            "home_team_glicko_rating": home.get_rating(),
            "home_team_rd": home.get_rd(),
            "home_team_vol": home.get_vol(),
            "away_team_glicko_rating": away.get_rating(),
            "away_team_rd": away.get_rd(),
            "away_team_vol": away.get_vol(),
        })

        # update ratings AFTER the match
        glicko.update_ratings(row[home_col], [row[away_col]], [home_score])
        glicko.update_ratings(row[away_col], [row[home_col]], [away_score])

    return pd.concat([df, pd.DataFrame(ratings_data, index=df.index)], axis=1)

# sync 1774962858137095699
# sync 1774962858864568994
# sys_sync_59f01b1d
# sys_sync_329277d5
# sys_sync_1bf122a8
