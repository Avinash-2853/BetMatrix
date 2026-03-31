title = "🏉 NFL Match Predictions 🏉"
submit_button = "Submit"
select_year_message = "Select Year"
select_week_message = "Select Week"
provide_column_list_message = "Please provide a list of columns to aggregate (stat_cols)."

# Scheduler Step Messages
STEP_1_STARTED = "Step 1: Loading data started..."
STEP_1_COMPLETED = "✅ Step 1 completed: Data loaded and saved to {}."
STEP_1_FAILED = "❌ Step 1 failed: {}"

STEP_2_STARTED = "Step 2: Data cleaning started..."
STEP_2_COMPLETED = "✅ Step 2 completed: Cleaned data saved to {}."
STEP_2_FAILED = "❌ Step 2 failed: {}"

STEP_3_STARTED = "Step 3: Feature engineering started..."
STEP_3_COMPLETED = "✅ Step 3 completed: Feature-engineered data saved to {}."
STEP_3_FAILED = "❌ Step 3 failed: {}"

STEP_4_STARTED = "Step 4: Data preprocessing started..."
STEP_4_COMPLETED = "✅ Step 4 completed: Preprocessed data saved. Scaler and encoders saved to {}."
STEP_4_FAILED = "❌ Step 4 failed: {}"

STEP_5_STARTED = "Step 5: Model training started..."
STEP_5_COMPLETED = "✅ Step 5 completed: Model trained and saved to {}."
STEP_5_FAILED = "❌ Step 5 failed: {}"

STEP_6_GENERATING = "Step 6: Generating weekly predictions for year {}..."
STEP_6_COMPLETED = "✅ Step 6 completed: Predictions generated for year {}"
STEP_6_FAILED = "❌ Step 6 failed: {}"
STEP_6_TRIGGERING_STEP_7 = "Triggering Step 7 to update match results after predictions generation..."
STEP_7_FAILED_AFTER_STEP_6 = "❌ Step 7 failed after Step 6: {}"

STEP_7_STARTED = "Step 7: Updating match results started..."
STEP_7_COMPLETED = "✅ Step 7 completed: Match results updated."
STEP_7_FAILED = "❌ Step 7 failed: {}"

STEP_8_STARTED = "Step 8: Updating future predictions started..."
STEP_8_COMPLETED = "✅ Step 8 completed: Future predictions updated."
STEP_8_FAILED = "❌ Step 8 failed: {}"

# Scheduling Messages
SCHEDULING_STEP_2 = "⏰ Scheduling Step 2 to run in 5 minutes..."
STEP_2_SCHEDULED = "✅ Step 2 scheduled to run at {}"

SCHEDULING_STEP_3 = "⏰ Scheduling Step 3 to run in 5 minutes..."
STEP_3_SCHEDULED = "✅ Step 3 scheduled to run at {}"

SCHEDULING_STEP_4 = "⏰ Scheduling Step 4 to run in 5 minutes..."
STEP_4_SCHEDULED = "✅ Step 4 scheduled to run at {}"

SCHEDULING_STEP_5 = "⏰ Scheduling Step 5 to run in 5 minutes..."
STEP_5_SCHEDULED = "✅ Step 5 scheduled to run at {}"

SCHEDULING_STEP_7 = "⏰ Scheduling Step 7 to run in 5 minutes..."
STEP_7_SCHEDULED = "✅ Step 7 scheduled to run at {}"

SCHEDULING_STEP_8 = "⏰ Scheduling Step 8 to run in 5 minutes..."
STEP_8_SCHEDULED = "✅ Step 8 scheduled to run at {}"

SCHEDULING_NEXT_CYCLE = "⏰ Scheduling Step 1 for next cycle at {}..."
NEXT_CYCLE_SCHEDULED = "✅ Next cycle scheduled to start at {}"

# Step 6 Waiting Messages
AUGUST_1ST_DETECTED = "August 1st detected and Step 6 is running. Waiting for Step 6 to complete..."
WAITING_FOR_STEP_6 = "Waiting for Step 6 to complete... ({}s elapsed)"
TIMEOUT_WAITING_STEP_6 = "Timeout waiting for Step 6. Proceeding with Step 7 anyway."

# Shutdown Messages
FORCE_SHUTDOWN = "Force shutdown requested. Exiting immediately."
RECEIVED_SIGNAL_SHUTDOWN = "Received {}. Initiating graceful shutdown..."
SCHEDULER_SHUTDOWN_COMPLETED = "Scheduler shutdown completed."
ERROR_DURING_SHUTDOWN = "Error during scheduler shutdown: {}"
SCHEDULER_STOPPED = "Scheduler stopped successfully."
UNEXPECTED_ERROR_MAIN_LOOP = "Unexpected error in main loop: {}"
SCHEDULER_SHUTDOWN_SUCCESS = "✅ Scheduler shutdown completed."
EXITING_MESSAGE = "👋 Exiting..."


# Startup Messages
SCHEDULER_STARTED = "⏳ Scheduler started. Pipeline runs weekly on Wednesday at 12:00 AM with 5-minute intervals between each step."
WEEKLY_PREDICTIONS_SCHEDULE = "📅 Predictions for entire year will be generated annually on August 1st at 12:00 AM."
MATCH_RESULTS_UPDATE_SCHEDULE = "🔄 Match results will be updated after Step 5 completes (or after Step 6 on August 1st)."
FUTURE_PREDICTIONS_UPDATE_SCHEDULE = "🔮 Future predictions will be updated 5 minutes after Step 7 completes."
GRACEFUL_SHUTDOWN_TIP = "💡 Press Ctrl+C or send SIGTERM for graceful shutdown."

# Lock File Messages
SCHEDULER_ALREADY_RUNNING = "ERROR: Scheduler already running with PID {}. Exiting."

# Model Training Logger Message
MODEL_TRAINING_PREFIX = "Model training: {}"

# ESPN API Error Messages (get_data.py)
ERROR_FETCHING_GAME_DATE = "Error fetching game date from ESPN API for game {}: {}"
ERROR_PARSING_GAME_DATE = "Error parsing game date for game {}: {}"
UNEXPECTED_ERROR_GAME_DATE = "An unexpected error occurred while fetching game date for game {}: {}"
ERROR_COMPETITORS_INCOMPLETE = "Error: Competitors data not found or incomplete for game {}."
ERROR_FETCHING_SCORE = "Error fetching score for game {}, team {}: {}"
ERROR_CANNOT_DETERMINE_SCORES = "Error: Could not determine both home and away scores for game {}."
ERROR_FETCHING_DATA_ESPN = "Error fetching data from ESPN API for game {}: {}"
ERROR_PARSING_JSON_DATA = "Error parsing the JSON data structure for game {}: {}"
UNEXPECTED_ERROR_GET_DATA = "An unexpected error occurred for game {}: {}"

# Data Processing Messages (schedule_scripts.py)
DATA_LOAD_STARTED = "Data load started..."
DATA_CLEANING_STARTED = "Data cleaning started..."
AGGREGATE_MATCH_FEATURES_STARTED = "Aggregate match features started..."
AGGREGATE_CATEGORICAL_FEATURES_STARTED = "Aggregate categorical features started..."
AGGREGATE_POSITIVE_NEGATIVE_STARTED = "Aggregate positive and negative features started..."
FILTER_GAME_STATS_STARTED = "Filter game stats started..."
REMOVE_COLUMNS_STARTED = "Remove columns started..."
NORMALIZE_YARDS_FEATURES_STARTED = "Normalize yards features started..."

# Feature Engineering Messages (schedule_scripts.py)
ADD_PREVIOUS_MATCH_RESULTS_STARTED = "Add previous match results started..."
REMOVE_DRAWS_STARTED = "Remove draws started..."
ADD_LAST_5_MATCHES_STATS_STARTED = "Add last 5 matches stats started..."
ADD_GLICKO_FEATURES_STARTED = "Add glicko features started..."
ADD_PREVIOUS_FEATURES_STARTED = "Add previous features started..."
ADD_LAST_5_H2H_WIN_RATIO_STARTED = "Add last 5 head to head win ratio of teams started..."
ADD_HISTORICAL_WIN_PCT_STARTED = "Add historical win percentage started..."
ADD_PF_PA_BY_SEASON_STARTED = "Add PF/PA by season started..."
ADD_HOME_AWAY_TEAM_AVG_SCORES_BEFORE_STARTED = "Add home/away team average scores before started..."
ADD_LEAGUE_AVG_SCORE_BEFORE_STARTED = "Add league average score before started..."
ADD_HOME_AWAY_TEAM_AVG_STAT_BEFORE_STARTED = "Add home/away team average stat before started..."
ADD_LEAGUE_AVG_STAT_BEFORE_STARTED = "Add league average stat before started..."

# Data Preprocessing Messages (schedule_scripts.py)
DATA_PREPROCESSING_STARTED = "Data preprocessing started..."
LABEL_ENCODING_STARTED = "Label encoding started..."
SCALING_STARTED = "Scaling started..."
SMOTE_STARTED = "SMOTE started..."

# Model Training Messages (schedule_scripts.py)
MODEL_TRAINING_STARTED = "Model training started..."
LOADING_MODEL_AND_ENCODERS = "Loading model and encoders..."
MODEL_AND_ENCODERS_LOADED = "Model and encoders loaded successfully"
CONNECTED_TO_DATABASE = "Connected to database: {}"

# Encoding Messages (schedule_scripts.py)
UNKNOWN_FEATURE_ENCODING = "Unknown {}: {}, using default encoding"

# Prediction Generation Messages (schedule_scripts.py)
SKIPPING_GAME_MISSING_DATA = "Skipping game {}: missing essential data"
PROCESSING_GAME = "Processing game: {} @ {} (Game ID: {})"
PREDICTION_STORED = "✅ Prediction stored for {} @ {}: Home={:.3f}, Away={:.3f}, Winner={}"
PROCESSING_WEEK = "Processing week {} of {}..."
NO_GAMES_FOUND = "No games found for year {}, week {}. Skipping..."
FOUND_GAMES_FOR_WEEK = "Found {} games for week {}"
ERROR_PROCESSING_GAME_TEMPLATE = "❌ Error processing game {}: {}"
ERROR_PROCESSING_GAME = ERROR_PROCESSING_GAME_TEMPLATE
WEEK_COMPLETED = "✅ Week {} completed: {} games processed"
STARTING_WEEKLY_PREDICTIONS = "Starting weekly predictions generation for year {}..."
MODEL_OR_ENCODER_NOT_FOUND = "❌ Model or encoder file not found: {}"
ERROR_LOADING_MODEL_ENCODERS = "❌ Error loading model/encoders: {}"
DATABASE_CONNECTION_FAILED = "❌ Database connection failed: {}"
ERROR_PROCESSING_WEEK = "❌ Error processing week {}: {}"
COMPLETED_PREDICTIONS = "✅ Completed predictions for year {}. Total games processed: {}"

# Match Results Update Messages (schedule_scripts.py)
STARTING_MATCH_RESULTS_UPDATE = "Starting match results update..."
CHECKING_WEEK = "Checking week {} of {}..."
NO_PREDICTIONS_FOUND = "No predictions found for year {}, week {}"
FOUND_PREDICTIONS_FOR_WEEK = "Found {} predictions for week {}"
GAME_ALREADY_HAS_SCORES = "Game {} already has scores (Home={}, Away={}). Skipping..."
CHECKING_GAME_DATE = "Checking game date for game {}..."
COULD_NOT_RETRIEVE_GAME_DATE = "⚠️ Could not retrieve game date for game {}. Skipping to avoid updating future games."
GAME_SCHEDULED_FUTURE = "⚠️ Game {} is scheduled for {} (future date). Skipping - game has not been played yet."
COULD_NOT_PARSE_GAME_DATE = "⚠️ Could not parse game date '{}' for game {}: {}. Skipping to be safe."
FETCHING_SCORES_FOR_GAME = "Fetching scores for game {} (played on {})..."
MATCH_SCORES_UNAVAILABLE = "⚠️ Match {} scores unavailable from API. Skipping update - leaving scores as NULL."
INCOMPLETE_SCORES = "⚠️ Incomplete scores for game {}: home={}, away={}. Skipping - leaving scores as NULL."
INVALID_SCORE_TYPES = "⚠️ Invalid score types for game {}: home={}, away={}. Skipping - leaving scores as NULL."
UPDATED_GAME = "✅ Updated game {}: Home={}, Away={}"
ERROR_PROCESSING_GAME_UPDATE = ERROR_PROCESSING_GAME_TEMPLATE
WEEK_PROCESSED = "✅ Week {} processed: {} games checked"
ERROR_PROCESSING_WEEK_UPDATE = "❌ Error processing week {}: {}"
MATCH_RESULTS_UPDATE_COMPLETED = "✅ Match results update completed. Updated: {}, Skipped: {}, Errors: {}"

# Prediction Accuracy Messages
CALCULATING_PREDICTION_ACCURACY = "📊 Calculating prediction accuracy for previous weeks..."
PREDICTION_ACCURACY_STARTED = "Starting prediction accuracy calculation..."
PREDICTION_ACCURACY_WEEK = "Week {}: {} correct out of {} games ({:.1f}% accuracy)"
PREDICTION_ACCURACY_SUMMARY = "📊 Prediction Accuracy Summary: {} correct out of {} total games ({:.1f}% overall accuracy)"
PREDICTION_ACCURACY_NO_DATA = "No completed games found for accuracy calculation"
PREDICTION_ACCURACY_COMPLETED = "✅ Prediction accuracy calculation completed"

# Future Predictions Update Messages (schedule_scripts.py)
STARTING_FUTURE_PREDICTIONS_UPDATE = "Starting future predictions update..."
FETCHING_ALL_PREDICTIONS = "Fetching all predictions from database..."
FOUND_PREDICTIONS_IN_DATABASE = "Found {} predictions in database"
COULD_NOT_RETRIEVE_GAME_DATE_FUTURE = "⚠️ Could not retrieve game date for game {}. Skipping."
GAME_PAST_OR_TODAY = "Game {} is on {} (past or today). Skipping - game may have been played."
UPDATING_PREDICTION_FOR_GAME = "Updating prediction for game {} (scheduled for {})..."
UPDATED_PREDICTION_FOR_GAME = "✅ Updated prediction for game {}: Home={:.3f}, Away={:.3f}, Winner={}"
COULD_NOT_PARSE_GAME_DATE_FUTURE = "⚠️ Could not parse game date '{}' for game {}: {}. Skipping."
ERROR_PROCESSING_GAME_FUTURE = ERROR_PROCESSING_GAME_TEMPLATE
FUTURE_PREDICTIONS_UPDATE_COMPLETED = "✅ Future predictions update completed. Updated: {}, Skipped: {}, Errors: {}"