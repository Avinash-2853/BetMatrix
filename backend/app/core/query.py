insert_prediction_data_query = """INSERT OR REPLACE INTO match_predictions (game_id,year, week,home_team, away_team, home_team_win_probability, away_team_win_probability, predicted_result, home_team_image_url, away_team_image_url, home_coach, away_coach, stadium)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

update_actual_result_query = """UPDATE match_predictions SET home_score = ?, away_score = ? WHERE game_id = ?"""

fetch_data_query = """SELECT game_id, year, week, home_team, away_team, home_team_win_probability, away_team_win_probability, predicted_result, home_team_image_url, away_team_image_url, home_coach, away_coach, stadium, home_score, away_score FROM match_predictions WHERE year = ? AND week = ?"""

fetch_match_scores_query = """SELECT game_id, home_score, away_score FROM match_predictions WHERE game_id = ?"""

fetch_prediction_by_game_id_query = """SELECT game_id, year, week, home_team, away_team, home_team_win_probability, away_team_win_probability, predicted_result, home_team_image_url, away_team_image_url, home_coach, away_coach, stadium, home_score, away_score FROM match_predictions WHERE game_id = ?"""

fetch_all_predictions_query = """SELECT game_id, year, week, home_team, away_team, home_coach, away_coach, stadium FROM match_predictions"""

update_probabilities_query = """UPDATE match_predictions SET home_team_win_probability = ?, away_team_win_probability = ?, predicted_result = ?, home_team = ?, away_team = ?, home_coach = ?, away_coach = ?, stadium = ?, home_team_image_url = ?, away_team_image_url = ? WHERE game_id = ?"""# sync 1774962858616762956
# sys_sync_1c35583
