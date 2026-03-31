import time
import pandas as pd
import logging
import os
import sys
import signal
import atexit

# Add backend directory to Python path BEFORE imports
current_dir = os.path.dirname(os.path.abspath(__file__))
# Go up two levels (from 'pipeline' to 'app', then from 'app' to 'backend')
backend_root = os.path.abspath(os.path.join(current_dir, "..", "..")) 
if backend_root not in sys.path:
    sys.path.insert(0, backend_root)

from dotenv import load_dotenv
import pickle
from apscheduler.schedulers.background import BackgroundScheduler  # type: ignore
from apscheduler.triggers.interval import IntervalTrigger  # type: ignore
from apscheduler.triggers.cron import CronTrigger  # type: ignore
from apscheduler.triggers.date import DateTrigger  # type: ignore
from apscheduler.jobstores.memory import MemoryJobStore  # type: ignore
from app.pipeline.schedule_scripts import data_load, data_cleaning, feature_engineering, data_preprocessing, model_train, generate_weekly_predictions, update_match_results, update_future_predictions
from app.core.constant import (
    STEP_1_STARTED, STEP_1_COMPLETED, STEP_1_FAILED,
    STEP_2_STARTED, STEP_2_COMPLETED, STEP_2_FAILED,
    STEP_3_STARTED, STEP_3_COMPLETED, STEP_3_FAILED,
    STEP_4_STARTED, STEP_4_COMPLETED, STEP_4_FAILED,
    STEP_5_STARTED, STEP_5_COMPLETED, STEP_5_FAILED,
    STEP_6_GENERATING, STEP_6_COMPLETED, STEP_6_FAILED, STEP_6_TRIGGERING_STEP_7, STEP_7_FAILED_AFTER_STEP_6,
    STEP_7_STARTED, STEP_7_COMPLETED, STEP_7_FAILED,
    STEP_8_STARTED, STEP_8_COMPLETED, STEP_8_FAILED,
    SCHEDULING_STEP_2, STEP_2_SCHEDULED,
    SCHEDULING_STEP_3, STEP_3_SCHEDULED,
    SCHEDULING_STEP_4, STEP_4_SCHEDULED,
    SCHEDULING_STEP_5, STEP_5_SCHEDULED,
    SCHEDULING_STEP_7, STEP_7_SCHEDULED,
    SCHEDULING_STEP_8, STEP_8_SCHEDULED,
    SCHEDULING_NEXT_CYCLE, NEXT_CYCLE_SCHEDULED,
    AUGUST_1ST_DETECTED, WAITING_FOR_STEP_6, TIMEOUT_WAITING_STEP_6,
    FORCE_SHUTDOWN, RECEIVED_SIGNAL_SHUTDOWN, SCHEDULER_SHUTDOWN_COMPLETED,
    ERROR_DURING_SHUTDOWN, SCHEDULER_STOPPED, UNEXPECTED_ERROR_MAIN_LOOP,
    SCHEDULER_STARTED, WEEKLY_PREDICTIONS_SCHEDULE, MATCH_RESULTS_UPDATE_SCHEDULE,
    FUTURE_PREDICTIONS_UPDATE_SCHEDULE, GRACEFUL_SHUTDOWN_TIP,
    SCHEDULER_ALREADY_RUNNING, MODEL_TRAINING_PREFIX,
    SCHEDULER_SHUTDOWN_SUCCESS, EXITING_MESSAGE
)

# Load environment variables
_ = load_dotenv()

# Module-level storage for Step 4 preprocessed data (to avoid CSV loading in Step 5)
# This allows Step 5 to use data directly from Step 4 when available
_step4_data_cache = {
    'x_resampled': None,
    'y_resampled': None,
    'x_test': None,
    'y_test': None,
    'scaler': None,
    'coach_le': None,
    'team_le': None,
    'ground_le': None
}

# Custom stream class to capture output and log it
class LoggerWriter:
    def __init__(self, logger, level=logging.INFO):
        self.logger = logger
        self.level = level
        self.buffer = ''

    def write(self, message):
        if message.strip():
            # Log each line
            for line in message.rstrip().split('\n'):
                if line.strip():
                    self.logger.log(self.level, MODEL_TRAINING_PREFIX.format(line))

    def flush(self):
        # Flush is required by file-like interface but no buffering is used,
        # so no action is needed
        pass

# -------------------------------
# Setup logging
# -------------------------------
log_file = os.getenv("scheduler_log_path")
if not log_file:
    log_file = os.path.join(backend_root, "logs", "scheduler.log")
else:
    # If .env path is relative, make it relative to backend_root
    if not os.path.isabs(log_file):
        log_file = os.path.join(backend_root, log_file)

# Create log directory if it doesn't exist
os.makedirs(os.path.dirname(log_file), exist_ok=True)

# Configure logging to both file and console
# Use 'a' mode to append to existing log file, and ensure UTF-8 encoding
file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter("%(asctime)s — %(levelname)s — %(message)s"))

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter("%(asctime)s — %(levelname)s — %(message)s"))

# Configure root logger
logging.basicConfig(
    level=logging.INFO,
    handlers=[file_handler, console_handler]
)
logger = logging.getLogger(__name__)

# Ensure logs are flushed immediately
file_handler.flush()

# -------------------------------
# File paths
# -------------------------------
BASE_PATH = os.getenv("data_input_path")
if not BASE_PATH:
    BASE_PATH = os.path.join(backend_root, "data", "input")
else:
    # If .env path is relative, make it relative to backend_root
    if not os.path.isabs(BASE_PATH):
        BASE_PATH = os.path.join(backend_root, BASE_PATH)

# Create data input directory if it doesn't exist
os.makedirs(BASE_PATH, exist_ok=True)
# Models directory path
MODELS_PATH = os.getenv("models_path")
if not MODELS_PATH:
    MODELS_PATH = os.path.join(backend_root, "models")
else:
    # If .env path is relative, make it relative to backend_root
    if not os.path.isabs(MODELS_PATH):
        MODELS_PATH = os.path.join(backend_root, MODELS_PATH)

# Create models directory if it doesn't exist
os.makedirs(MODELS_PATH, exist_ok=True)

RAW_CSV = os.path.join(BASE_PATH, "nfl_using_nflreadpy.csv")
CLEANED_CSV = os.path.join(BASE_PATH, "step2_nfl_cleaned_data.csv")
FEATURE_CSV = os.path.join(BASE_PATH, "step3_nfl_processed_data.csv")
PREPROCESSED_FEATURE_CSV = os.path.join(BASE_PATH, "step4_nfl_data_preprocessed_features.csv")
PREPROCESSED_TARGET_CSV = os.path.join(BASE_PATH, "step4_nfl_data_preprocessed_target.csv")
PREPROCESSED_TEST_FEATURE_CSV = os.path.join(BASE_PATH, "step4_nfl_data_preprocessed_test_features.csv")
PREPROCESSED_TEST_TARGET_CSV = os.path.join(BASE_PATH, "step4_nfl_data_preprocessed_test_target.csv")

# Model file paths
MODEL_FILE = os.path.join(MODELS_PATH, "cat_model.pkl")
SCALER_FILE = os.path.join(MODELS_PATH, "scaler.pkl")
COACH_ENCODER_FILE = os.path.join(MODELS_PATH, "coach_encoder.pkl")
TEAM_ENCODER_FILE = os.path.join(MODELS_PATH, "team_encoder.pkl")
GROUND_ENCODER_FILE = os.path.join(MODELS_PATH, "ground_encoder.pkl")

# -------------------------------
# Step 1: Data Load
# -------------------------------
# Track cycle start time for calculating next cycle
cycle_start_time = None

def step1_data_load():
    global cycle_start_time
    try:
        # Track cycle start time
        cycle_start_time = pd.Timestamp.now()
        logger.info(STEP_1_STARTED)
        df = data_load()
        df.write_csv(RAW_CSV)
        logger.info(STEP_1_COMPLETED.format(RAW_CSV))
        
        # Schedule Step 2 to run 5 minutes after Step 1 completes
        logger.info(SCHEDULING_STEP_2)
        step2_trigger_time = pd.Timestamp.now() + pd.Timedelta(minutes=5)
        scheduler.add_job(
            step2_data_cleaning,
            DateTrigger(run_date=step2_trigger_time),
            id='step2_data_cleaning_triggered',
            name='Step 2: Data Cleaning (Triggered)',
            replace_existing=True
        )
        logger.info(STEP_2_SCHEDULED.format(step2_trigger_time))
    except Exception as e:
        logger.exception(STEP_1_FAILED.format(e))

# -------------------------------
# Step 2: Data Cleaning
# -------------------------------
def step2_data_cleaning():
    try:
        logger.info(STEP_2_STARTED)
        df = pd.read_csv(RAW_CSV, low_memory=False)
        df1 = data_cleaning(df)
        df1.to_csv(CLEANED_CSV, index=False)
        logger.info(STEP_2_COMPLETED.format(CLEANED_CSV))
        
        # Schedule Step 3 to run 5 minutes after Step 2 completes
        logger.info(SCHEDULING_STEP_3)
        step3_trigger_time = pd.Timestamp.now() + pd.Timedelta(minutes=5)
        scheduler.add_job(
            step3_feature_engineering,
            DateTrigger(run_date=step3_trigger_time),
            id='step3_feature_engineering_triggered',
            name='Step 3: Feature Engineering (Triggered)',
            replace_existing=True
        )
        logger.info(STEP_3_SCHEDULED.format(step3_trigger_time))
    except Exception as e:
        logger.exception(STEP_2_FAILED.format(e))

# -------------------------------
# Step 3: Feature Engineering
# -------------------------------
def step3_feature_engineering():
    try:
        logger.info(STEP_3_STARTED)
        df1 = pd.read_csv(CLEANED_CSV, low_memory=False)
        df2 = feature_engineering(df1)
        df2.to_csv(FEATURE_CSV, index=False)
        logger.info(STEP_3_COMPLETED.format(FEATURE_CSV))
        
        # Schedule Step 4 to run 5 minutes after Step 3 completes
        logger.info(SCHEDULING_STEP_4)
        step4_trigger_time = pd.Timestamp.now() + pd.Timedelta(minutes=5)
        scheduler.add_job(
            step4_data_preprocessing,
            DateTrigger(run_date=step4_trigger_time),
            id='step4_data_preprocessing_triggered',
            name='Step 4: Data Preprocessing (Triggered)',
            replace_existing=True
        )
        logger.info(STEP_4_SCHEDULED.format(step4_trigger_time))
    except Exception as e:
        logger.exception(STEP_3_FAILED.format(e))

# -------------------------------
# Step 4: Data Preprocessing
# -------------------------------
def step4_data_preprocessing():
    global _step4_data_cache
    try:
        logger.info(STEP_4_STARTED)
        df2 = pd.read_csv(FEATURE_CSV, low_memory=False)
        x_resampled, y_resampled, scaler, coach_le, team_le, ground_le, x_test, y_test = data_preprocessing(df2)
        
        # Store preprocessed data in module-level cache for direct use in Step 5
        _step4_data_cache['x_resampled'] = x_resampled
        _step4_data_cache['y_resampled'] = y_resampled
        _step4_data_cache['x_test'] = x_test
        _step4_data_cache['y_test'] = y_test
        _step4_data_cache['scaler'] = scaler
        _step4_data_cache['coach_le'] = coach_le
        _step4_data_cache['team_le'] = team_le
        _step4_data_cache['ground_le'] = ground_le
        
        logger.info("✅ Preprocessed data stored in memory for direct use in Step 5")
        
        # Save training data (resampled) - still save to CSV for persistence/recovery
        x_resampled.to_csv(PREPROCESSED_FEATURE_CSV, index=False)
        y_resampled.to_csv(PREPROCESSED_TARGET_CSV, index=False)
        
        # Save test data
        x_test.to_csv(PREPROCESSED_TEST_FEATURE_CSV, index=False)
        y_test.to_csv(PREPROCESSED_TEST_TARGET_CSV, index=False)
        
        # Save scaler and encoders using pickle
        with open(SCALER_FILE, 'wb') as f:
            pickle.dump(scaler, f)
        with open(COACH_ENCODER_FILE, 'wb') as f:
            pickle.dump(coach_le, f)
        with open(TEAM_ENCODER_FILE, 'wb') as f:
            pickle.dump(team_le, f)
        with open(GROUND_ENCODER_FILE, 'wb') as f:
            pickle.dump(ground_le, f)
        
        logger.info(STEP_4_COMPLETED.format(MODELS_PATH))
        
        # Schedule Step 5 to run 5 minutes after Step 4 completes
        logger.info(SCHEDULING_STEP_5)
        step5_trigger_time = pd.Timestamp.now() + pd.Timedelta(minutes=5)
        scheduler.add_job(
            step5_model_train,
            DateTrigger(run_date=step5_trigger_time),
            id='step5_model_train_triggered',
            name='Step 5: Model Training (Triggered)',
            replace_existing=True
        )
        logger.info(STEP_5_SCHEDULED.format(step5_trigger_time))
    except Exception as e:
        logger.exception(STEP_4_FAILED.format(e))
        # Clear cache on error
        _step4_data_cache = dict.fromkeys(_step4_data_cache, None)

def step5_model_train():
    try:
        logger.info(STEP_5_STARTED)
        X_train, y_train, X_test, y_test_series = _prepare_step5_datasets()
        logger.info(f"Training data: {X_train.shape[0]} samples, Test data: {X_test.shape[0]} samples")
        _train_and_save_model(X_train, y_train, X_test, y_test_series)
        _schedule_step7()
    except Exception as e:
        logger.exception(STEP_5_FAILED.format(e))


def _prepare_step5_datasets():
    """Return train/test data either from cache or disk."""
    if _is_step4_cache_ready():
        logger.info("✅ Using preprocessed data directly from Step 4 (in-memory)")
        return _consume_step4_cache()
    logger.info("⚠️  Step 4 data not in memory, loading from CSV files")
    return _load_step4_outputs_from_disk()


def _is_step4_cache_ready() -> bool:
    """Check whether all required Step 4 artifacts are cached."""
    required_keys = ("x_resampled", "y_resampled", "x_test", "y_test")
    return all(_step4_data_cache.get(key) is not None for key in required_keys)


def _consume_step4_cache():
    """Extract and clear cached Step 4 data."""
    global _step4_data_cache
    X_train = _step4_data_cache["x_resampled"]
    y_resampled = _step4_data_cache["y_resampled"]
    X_test = _step4_data_cache["x_test"]
    y_test = _step4_data_cache["y_test"]
    _step4_data_cache = dict.fromkeys(_step4_data_cache, None)
    y_train = _extract_series_or_array(y_resampled)
    y_test_series = _extract_series_or_array(y_test)
    return X_train, y_train, X_test, y_test_series


def _load_step4_outputs_from_disk():
    """Load Step 4 outputs from persisted CSV files."""
    X_train = pd.read_csv(PREPROCESSED_FEATURE_CSV, low_memory=False)
    y_train_df = pd.read_csv(PREPROCESSED_TARGET_CSV, low_memory=False)
    X_test = pd.read_csv(PREPROCESSED_TEST_FEATURE_CSV, low_memory=False)
    y_test_df = pd.read_csv(PREPROCESSED_TEST_TARGET_CSV, low_memory=False)
    y_train = _extract_series_or_array(y_train_df)
    y_test_series = _extract_series_or_array(y_test_df)
    return X_train, y_train, X_test, y_test_series


def _extract_series_or_array(values):
    """Normalize pandas containers for downstream functions."""
    if isinstance(values, pd.DataFrame):
        return values.iloc[:, 0] if len(values.columns) > 0 else values.squeeze()
    return values


def _train_and_save_model(X_train, y_train, X_test, y_test_series):
    """Train the CatBoost model with logging capture and persist it."""
    stdout_logger = LoggerWriter(logger, logging.INFO)
    stderr_logger = LoggerWriter(logger, logging.WARNING)
    original_stdout = sys.stdout
    original_stderr = sys.stderr
    
    try:
        sys.stdout = stdout_logger
        sys.stderr = stderr_logger
        model = model_train(X_train, y_train, x_test=X_test, y_test=y_test_series)
    finally:
        sys.stdout = original_stdout
        sys.stderr = original_stderr
    
    with open(MODEL_FILE, 'wb') as f:
        pickle.dump(model, f)
    
    logger.info(STEP_5_COMPLETED.format(MODEL_FILE))


def _schedule_step7():
    """Schedule Step 7 to run five minutes after Step 5 completes."""
    logger.info(SCHEDULING_STEP_7)
    step7_trigger_time = pd.Timestamp.now() + pd.Timedelta(minutes=5)
    scheduler.add_job(
        step7_update_match_results,
        DateTrigger(run_date=step7_trigger_time),
        id='step7_update_match_results_triggered',
        name='Step 7: Update Match Results (Triggered)',
        replace_existing=True,
    )
    logger.info(STEP_7_SCHEDULED.format(step7_trigger_time))

# -------------------------------
# Prevent Multiple Instances
# -------------------------------
# Create a lock file to prevent multiple scheduler instances
lock_file = os.path.join(backend_root, "logs", "scheduler.lock")
if os.path.exists(lock_file):
    # Check if the process is still running
    try:
        with open(lock_file, 'r') as f:
            old_pid = int(f.read().strip())
        # Check if process exists
        os.kill(old_pid, 0)  # Will raise OSError if process doesn't exist
        # Process exists - just log a warning but continue running
        warning_msg = f"⚠️  Another scheduler instance may be running (PID: {old_pid}). Continuing anyway..."
        try:
            logger.warning(warning_msg)
        except Exception:
            print(warning_msg)
    except (OSError, ValueError):
        # Process doesn't exist, remove stale lock file
        os.remove(lock_file)

# Create lock file with current PID
os.makedirs(os.path.dirname(lock_file), exist_ok=True)
with open(lock_file, 'w') as f:
    f.write(str(os.getpid()))

# Remove lock file on exit
def cleanup_lock():
    if os.path.exists(lock_file):
        try:
            os.remove(lock_file)
        except OSError:
            pass

atexit.register(cleanup_lock)

# -------------------------------
# Scheduler Setup
# -------------------------------
# Use memory job store to avoid persistence issues and prevent duplicates
scheduler = BackgroundScheduler(
    jobstores={'default': MemoryJobStore()},
    coalesce=True,  # Combine multiple pending executions into one
    max_workers=10
)

# Schedule Step 1 to run daily at 11:45 AM
# Steps 2-8 will be triggered sequentially by previous steps with 5-minute intervals
# After Step 8 completes, the cycle ends. Next cycle will start next day at 11:45 AM
scheduler.add_job(
    step1_data_load,
    CronTrigger(hour=0, minute=0),  # Daily at 12:00 AM
    id='step1_data_load_daily',
    name='Step 1: Data Load (Daily)',
    replace_existing=True
)

# Note: Steps 1-8 run daily at 12:00 AM:
# - Step 1 (Daily 12:00 AM) → triggers Step 2 (after 5 min)
# - Step 2 → triggers Step 3 (after 5 min)
# - Step 3 → triggers Step 4 (after 5 min)
# - Step 4 → triggers Step 5 (after 5 min)
# - Step 5 (Model Training) → triggers Step 7 (after 5 min)
# - Step 7 (Update Match Results) → triggers Step 8 (after 5 min)
# - Step 8 (Update Future Predictions) → completes cycle
# - Next cycle starts next day at 11:45 AM

# -------------------------------
# Step 6: Generate Weekly Predictions
# -------------------------------
def step6_generate_predictions():
    """
    Generate predictions for all weeks (1-18) of the current year.
    This runs annually on August 1st at 12:00 AM.
    After completion, triggers Step 7 to update match results.
    """
    global step6_running
    step6_running = True
    
    try:
        current_year = time.localtime().tm_year
        logger.info(STEP_6_GENERATING.format(current_year))
        generate_weekly_predictions(current_year)
        logger.info(STEP_6_COMPLETED.format(current_year))
        
        # Mark Step 6 as completed
        step6_running = False
        
        # Trigger Step 7 to run after Step 6 completes
        logger.info(STEP_6_TRIGGERING_STEP_7)
        try:
            step7_update_match_results()
        except Exception as e:
            logger.exception(STEP_7_FAILED_AFTER_STEP_6.format(e))
    except Exception as e:
        step6_running = False
        logger.exception(STEP_6_FAILED.format(e))
        raise

# Schedule Step 6 to run annually on August 1st at 12:00 AM
scheduler.add_job(
    step6_generate_predictions,
    CronTrigger(month=8, day=1, hour=0, minute=0),  # August 1st at 12:00 AM
    id='step6_generate_predictions_august',
    name='Step 6: Generate Weekly Predictions (August 1st)',
    replace_existing=True
)

# -------------------------------
# Step 7: Update Match Results
# -------------------------------
# Global flag to track if Step 6 is running
step6_running = False

def step7_update_match_results():
    """
    Update actual match results in the database for completed games.
    
    This function intelligently runs:
    - After Step 6 completes (called directly from Step 6 on August 1st)
    - After Step 5 completes (triggered by Step 5 in daily schedule)
    
    If called from scheduler and Step 6 is running, it waits for Step 6 to complete.
    After completing, it triggers Step 8 to run after 5 minutes.
    """
    global step6_running
    
    try:
        # Check if Step 6 is currently running (on August 1st)
        current_date = pd.Timestamp.now()
        is_august_1st_early = current_date.month == 8 and current_date.day == 1 and current_date.hour < 2
        
        if is_august_1st_early and step6_running:
            # On August 1st early morning, Step 6 might still be running
            logger.info(AUGUST_1ST_DETECTED)
            
            max_wait_time = 3600  # Wait up to 1 hour for Step 6
            wait_interval = 30  # Check every 30 seconds
            waited = 0
            
            while step6_running and waited < max_wait_time:
                logger.info(WAITING_FOR_STEP_6.format(waited))
                time.sleep(wait_interval)
                waited += wait_interval
            
            if waited >= max_wait_time:
                logger.warning(TIMEOUT_WAITING_STEP_6)
        
        logger.info(STEP_7_STARTED)
        update_match_results()
        logger.info(STEP_7_COMPLETED)
        
        # Note: Prediction accuracy is automatically calculated and logged 
        # inside update_match_results() function
        
        # Trigger Step 8 to run 5 minutes after Step 7 completes
        logger.info(SCHEDULING_STEP_8)
        step8_trigger_time = pd.Timestamp.now() + pd.Timedelta(minutes=5)
        scheduler.add_job(
            step8_update_future_predictions,
            DateTrigger(run_date=step8_trigger_time),
            id='step8_update_future_predictions_triggered',
            name='Step 8: Update Future Predictions (Triggered)',
            replace_existing=True
        )
        logger.info(STEP_8_SCHEDULED.format(step8_trigger_time))
        
    except Exception as e:
        logger.exception(STEP_7_FAILED.format(e))

# Note: Step 7 is now triggered by Step 5 (with 5-minute delay)
# It will also be triggered directly by Step 6 when it completes on August 1st
# No separate IntervalTrigger schedule needed - it's chained from Step 5

def step8_update_future_predictions():
    """
    Step 8: Update probabilities for future games.
    
    This function updates home_team_win_probability and away_team_win_probability
    for games that are scheduled in the future (game date > current date).
    Completes the daily pipeline cycle. Next cycle will start next day at 11:45 AM.
    """
    try:
        logger.info(STEP_8_STARTED)
        update_future_predictions()
        logger.info(STEP_8_COMPLETED)
        logger.info("✅ Daily pipeline cycle completed. Next cycle will run next day at 11:45 AM.")
        
    except Exception as e:
        logger.exception(STEP_8_FAILED.format(e))

# Note: Step 8 is now triggered directly by Step 7 after it completes (with 5-minute delay)
# After Step 8 completes, the daily cycle ends. Next cycle starts next day at 11:45 AM
# No separate IntervalTrigger schedule needed - it's chained from Step 7

# Signal handler for graceful shutdown
def signal_handler(signum=None, frame=None):
    """Handle signals by shutting down scheduler gracefully."""
    if signum:
        signal_names = {
            signal.SIGINT: "SIGINT",
            signal.SIGTERM: "SIGTERM",
        }
        signal_name = signal_names.get(signum, f"Signal {signum}")
        logger.info(f"⚠️  Received {signal_name} signal. Shutting down scheduler...")
    else:
        logger.info("⚠️  Signal received. Shutting down scheduler...")
    
    # Shutdown scheduler gracefully
    try:
        scheduler.shutdown(wait=True)
        logger.info(SCHEDULER_SHUTDOWN_SUCCESS)
    except Exception as shutdown_error:
        logger.exception(f"❌ Error during scheduler shutdown: {shutdown_error}")
    
    logger.info(EXITING_MESSAGE)
    sys.exit(0)

# Register signal handlers for graceful shutdown
signal.signal(signal.SIGINT, signal_handler)   # Ctrl+C
signal.signal(signal.SIGTERM, signal_handler)   # kill command

# Log startup message to both console and file
logger.info("=" * 70)
logger.info("🚀 Starting NFL Sports Prediction Scheduler...")
logger.info(f"📝 Logs will be written to: {log_file}")
logger.info("=" * 70)

logger.info(SCHEDULER_STARTED)
logger.info(WEEKLY_PREDICTIONS_SCHEDULE)
logger.info(MATCH_RESULTS_UPDATE_SCHEDULE)
logger.info(FUTURE_PREDICTIONS_UPDATE_SCHEDULE)
logger.info("🔄 Scheduler is running. Press Ctrl+C to stop gracefully.")

try:
    logger.info("🔄 Starting scheduler...")
    scheduler.start()
    logger.info("✅ Scheduler started successfully!")

    # Skip infinite loop if running in test mode (for pytest)
    if os.getenv("SKIP_SCHEDULER_LOOP") == "1":
        logger.info("⚠️  Test mode detected (SKIP_SCHEDULER_LOOP=1) - skipping infinite loop")
    else:
        logger.info("🔄 Scheduler is running. Press Ctrl+C to stop the scheduler...")
        logger.info("📊 Waiting for scheduled jobs to execute...")
        logger.info("🔄 Entering infinite loop to keep scheduler alive...")
        # Infinite loop - scheduler runs forever
        while True:
            try:
                time.sleep(10)
            except KeyboardInterrupt:
                # Exit on KeyboardInterrupt
                logger.info("⚠️  KeyboardInterrupt received. Shutting down scheduler...")
                try:
                    scheduler.shutdown(wait=True)
                    logger.info(SCHEDULER_SHUTDOWN_SUCCESS)
                except Exception as shutdown_error:
                    logger.exception(f"❌ Error during scheduler shutdown: {shutdown_error}")
                logger.info(EXITING_MESSAGE)
                sys.exit(0)
            except Exception as e:
                # Log error but continue running
                logger.exception(f"⚠️  Unexpected error in main loop: {e}. Scheduler continues running...")
                time.sleep(10)  # Wait before continuing
                continue
except Exception as e:
    logger.exception(f"❌ Failed to start scheduler: {e}")
    logger.info("🔄 Retrying scheduler startup in 60 seconds...")
    # Retry logic - keep trying to start scheduler
    while True:
        try:
            time.sleep(60)
            logger.info("🔄 Attempting to restart scheduler...")
            scheduler.start()
            logger.info("✅ Scheduler restarted successfully!")
            # If successful, enter the main loop
            while True:
                try:
                    time.sleep(10)
                except KeyboardInterrupt:
                    # Exit on KeyboardInterrupt
                    logger.info("⚠️  KeyboardInterrupt received. Shutting down scheduler...")
                    try:
                        scheduler.shutdown(wait=True)
                        logger.info(SCHEDULER_SHUTDOWN_SUCCESS)
                    except Exception as shutdown_error:
                        logger.exception(f"❌ Error during scheduler shutdown: {shutdown_error}")
                    logger.info(EXITING_MESSAGE)
                    sys.exit(0)
                except Exception as retry_error:
                    logger.exception(f"⚠️  Unexpected error in main loop: {retry_error}. Scheduler continues running...")
                    time.sleep(10)
                    continue
        except Exception as retry_error:
            logger.exception(f"❌ Failed to restart scheduler: {retry_error}. Retrying in 60 seconds...")
            continue
# sync 1774962858539791821
# sync 1774962859444904921
