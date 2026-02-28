# type: ignore
import sys
import os
import math
import pytest
import pandas as pd
from typing import Any
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from app.pipeline.glicko import (
    Glicko2Player,
    Glicko2,
    add_glicko_features
)


# ----------------------------
# Glicko2Player tests
# ----------------------------
def test_glicko2player_default_conversion() -> None:
    """Test default player rating, RD, and volatility conversions."""
    p = Glicko2Player()  # default 1500, rd=350, vol=0.06
    assert pytest.approx(1500.0, rel=1e-6) == p.get_rating()
    assert pytest.approx(350.0, rel=1e-6) == p.get_rd()
    assert pytest.approx(0.06, rel=1e-12) == p.get_vol()


def test_glicko2player_custom() -> None:
    """Test custom initialization of player rating, RD, and volatility."""
    p = Glicko2Player(rating=1600, rd=200, vol=0.12)
    assert pytest.approx(1600.0, rel=1e-6) == p.get_rating()
    assert pytest.approx(200.0, rel=1e-6) == p.get_rd()
    assert pytest.approx(0.12, rel=1e-12) == p.get_vol()


# ----------------------------
# Glicko2 internal helpers
# ----------------------------
def test_g_and_e_behavior() -> None:
    """Test _g() and _expected_score() helper functions."""
    gsys = Glicko2(tau=0.5)
    assert pytest.approx(1.0, rel=1e-12) == gsys._g(0.0)

    mu = 0.0
    mu_j = 0.0
    phi_j = 0.0
    assert pytest.approx(0.5, rel=1e-12) == gsys._expected_score(mu, mu_j, phi_j)


# ----------------------------
# _update_sigma branch tests
# ----------------------------
def test_update_sigma_large_delta_branch() -> None:
    """Trigger the branch where delta^2 > (phi^2 + v) and return positive sigma."""
    gsys = Glicko2(tau=0.5)
    player = Glicko2Player()
    delta = 20.0
    v = 0.01
    sigma_new = gsys._update_sigma(player, delta, v)
    assert isinstance(sigma_new, float)
    assert sigma_new > 0.0


def test_update_sigma_else_branch_iteration() -> None:
    """Trigger the else branch iteration and ensure sigma convergence."""
    gsys = Glicko2(tau=0.3)
    player = Glicko2Player(rating=1500, rd=30, vol=0.06)
    delta = 0.001
    v = 5.0
    sigma_new = gsys._update_sigma(player, delta, v)
    assert isinstance(sigma_new, float)
    assert sigma_new > 0.0


# ----------------------------
# update_ratings tests
# ----------------------------
def test_update_ratings_no_games_early_return() -> None:
    """Ensure update_ratings returns early if no opponents are provided."""
    gsys = Glicko2()
    _ = gsys.ensure_player("Alice")
    before_mu = gsys.get_player("Alice").mu
    gsys.update_ratings("Alice", [], [])
    assert gsys.get_player("Alice").mu == before_mu


def test_update_ratings_symmetry_and_change() -> None:
    """
    Update ratings for player A against B and ensure mu/phi/sigma update.
    """
    gsys = Glicko2()
    _ = gsys.ensure_player("A")
    _ = gsys.ensure_player("B")

    a_before = gsys.get_player("A")
    mu_a_before = a_before.mu
    phi_a_before = a_before.phi
    sigma_a_before = a_before.sigma

    gsys.update_ratings("A", ["B"], [1.0])
    a_after = gsys.get_player("A")

    assert hasattr(a_after, "mu") and hasattr(a_after, "phi") and hasattr(a_after, "sigma")
    assert not math.isnan(a_after.mu)
    assert isinstance(a_after.mu, float)
    assert isinstance(a_after.phi, float)
    assert isinstance(a_after.sigma, float)

    # Check that ratings actually changed
    assert a_after.mu != mu_a_before
    assert a_after.phi != phi_a_before
    assert a_after.sigma != sigma_a_before


# ----------------------------
# add_glicko_features tests
# ----------------------------
def test_add_glicko_features_three_outcomes_and_columns() -> None:
    """
    Test add_glicko_features with home win, away win, and draw.
    Ensure proper columns are added and initial ratings are correct.
    """
    df = pd.DataFrame({
        "home_team": ["T1", "T2", "T3"],
        "away_team": ["T2", "T3", "T1"],
        "total_home_score": [5, 2, 10],
        "total_away_score": [2, 3, 10],
    })

    out = add_glicko_features(
        df.copy(),
        home_col="home_team",
        away_col="away_team",
        home_score_col="total_home_score",
        away_score_col="total_away_score"
    )

    expected_cols = [
        "home_team_glicko_rating",
        "home_team_rd",
        "home_team_vol",
        "away_team_glicko_rating",
        "away_team_rd",
        "away_team_vol",
    ]
    for c in expected_cols:
        assert c in out.columns

    # Check initial ratings
    assert out.loc[0, "home_team_glicko_rating"] == pytest.approx(1500.0)
    assert out.loc[0, "away_team_glicko_rating"] == pytest.approx(1500.0)

    # Ensure values are finite
    assert out.shape[0] == 3
    for c in expected_cols:
        assert math.isfinite(out[c].iloc[0])
# sync 1774962759569918889
# sys_sync_33936241
# sys_sync_cbd44ad
# sys_sync_59455d7d
# sys_sync_67056154
# sys_sync_664164be
