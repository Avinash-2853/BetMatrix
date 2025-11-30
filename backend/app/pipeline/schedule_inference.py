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
from app.pipeline.schedule_scripts import data_load, data_cleaning, feature_engineering, generate_weekly_predictions, update_match_results, update_future_predictions
from app.core.constant import (
    STEP_1_STARTED, STEP_1_COMPLETED, STEP_1_FAILED,
    STEP_2_STARTED, STEP_2_COMPLETED, STEP_2_FAILED,
    STEP_3_STARTED, STEP_3_COMPLETED, STEP_3_FAILED,
    STEP_6_GENERATING, STEP_6_COMPLETED, STEP_6_FAILED, STEP_6_TRIGGERING_STEP_7, STEP_7_FAILED_AFTER_STEP_6,
    STEP_7_STARTED, STEP_7_COMPLETED, STEP_7_FAILED,
    STEP_8_STARTED, STEP_8_COMPLETED, STEP_8_FAILED,
    SCHEDULING_STEP_2, STEP_2_SCHEDULED,
    SCHEDULING_STEP_3, STEP_3_SCHEDULED,
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
log_file = os.getenv("scheduler_inference_log_path")
if not log_file:
    log_file = os.path.join(backend_root, "logs", "scheduler_inference.log")
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
        
        # SKIP Step 4 (Preprocessing) and Step 5 (Training)
        # Directly schedule Step 7 to run 5 minutes after Step 3 completes
        logger.info("ℹ️  Skipping Step 4 & 5 (Training) - Moving to Match Results Update")
        logger.info(SCHEDULING_STEP_7)
        step7_trigger_time = pd.Timestamp.now() + pd.Timedelta(minutes=5)
        scheduler.add_job(
            step7_update_match_results,
            DateTrigger(run_date=step7_trigger_time),
            id='step7_update_match_results_triggered',
            name='Step 7: Update Match Results (Triggered)',
            replace_existing=True
        )
        logger.info(STEP_7_SCHEDULED.format(step7_trigger_time))
    except Exception as e:
        logger.exception(STEP_3_FAILED.format(e))

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
    - After Step 3 completes (triggered by Step 3 in daily schedule in this inference-only script)
    
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
        logger.info("✅ Daily inference pipeline cycle completed. Next cycle will run next day at 11:45 AM.")
        
    except Exception as e:
        logger.exception(STEP_8_FAILED.format(e))

# -------------------------------
# Prevent Multiple Instances
# -------------------------------
# Create a lock file to prevent multiple scheduler instances - DISTINCT FROM TRAINING SCHEDULER
lock_file = os.path.join(backend_root, "logs", "scheduler_inference.lock")
if os.path.exists(lock_file):
    # Check if the process is still running
    try:
        with open(lock_file, 'r') as f:
            old_pid = int(f.read().strip())
        # Check if process exists
        os.kill(old_pid, 0)  # Will raise OSError if process doesn't exist
        # Process exists - just log a warning but continue running
        warning_msg = f"⚠️  Another inference scheduler instance may be running (PID: {old_pid}). Continuing anyway..."
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
# Steps 2, 3, 7, 8 will be triggered sequentially by previous steps with 5-minute intervals
# Steps 4 and 5 (Training) are SKIPPED.
scheduler.add_job(
    step1_data_load,
    CronTrigger(hour=0, minute=0),  # Daily at 12:00 AM (assumes server time mapping, kept same as original)
    id='step1_data_load_daily',
    name='Step 1: Data Load (Daily)',
    replace_existing=True
)

# Schedule Step 6 to run annually on August 1st at 12:00 AM
scheduler.add_job(
    step6_generate_predictions,
    CronTrigger(month=8, day=1, hour=0, minute=0),  # August 1st at 12:00 AM
    id='step6_generate_predictions_august',
    name='Step 6: Generate Weekly Predictions (August 1st)',
    replace_existing=True
)

# Signal handler for graceful shutdown
def signal_handler(signum=None, frame=None):
    """Handle signals by shutting down scheduler gracefully."""
    if signum:
        signal_names = {
            signal.SIGINT: "SIGINT",
            signal.SIGTERM: "SIGTERM",
        }
        signal_name = signal_names.get(signum, f"Signal {signum}")
        logger.info(f"⚠️  Received {signal_name} signal. Shutting down inference scheduler...")
    else:
        logger.info("⚠️  Signal received. Shutting down inference scheduler...")
    
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
logger.info("🚀 Starting NFL Sports Prediction INFERENCE Scheduler...")
logger.info("ℹ️  Running in PREDICTION-ONLY mode (No Model Training)")
logger.info(f"📝 Logs will be written to: {log_file}")
logger.info("=" * 70)

logger.info(SCHEDULER_STARTED)
logger.info(WEEKLY_PREDICTIONS_SCHEDULE)
logger.info(MATCH_RESULTS_UPDATE_SCHEDULE)
logger.info(FUTURE_PREDICTIONS_UPDATE_SCHEDULE)
logger.info("🔄 Inference Scheduler is running. Press Ctrl+C to stop gracefully.")

try:
    logger.info("🔄 Starting scheduler...")
    scheduler.start()
    logger.info("✅ Inference Scheduler started successfully!")

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
    sys.exit(1)
# sync 1774962859051857191
# sync 1774962859672736415
# sys_sync_fba4fd3
# sys_sync_6ebe2cff
# sys_sync_312b61ce
# sys_sync_1d2d1abe
