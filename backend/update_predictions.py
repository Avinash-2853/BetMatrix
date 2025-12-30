import os
import sys
import pandas as pd
import sqlite3
import pickle
import logging
from typing import Union

# Add backend directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

from app.pipeline.schedule_scripts import (
    model_train, 
    generate_weekly_predictions, 
    update_future_predictions
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)
logger = logging.getLogger(__name__)

# Paths
BASE_PATH = os.path.join(current_dir, "data", "input")
MODELS_PATH = os.path.join(current_dir, "models")
DATABASE_PATH = os.path.join(current_dir, "data", "database", "nfl_predictions.db")

PREPROCESSED_FEATURE_CSV = os.path.join(BASE_PATH, "step4_nfl_data_preprocessed_features.csv")
PREPROCESSED_TARGET_CSV = os.path.join(BASE_PATH, "step4_nfl_data_preprocessed_target.csv")
PREPROCESSED_TEST_FEATURE_CSV = os.path.join(BASE_PATH, "step4_nfl_data_preprocessed_test_features.csv")
PREPROCESSED_TEST_TARGET_CSV = os.path.join(BASE_PATH, "step4_nfl_data_preprocessed_test_target.csv")
MODEL_FILE = os.path.join(MODELS_PATH, "cat_model.pkl")

def _extract_series_or_array(values):
    """Normalize pandas containers for downstream functions."""
    if isinstance(values, pd.DataFrame):
        return values.iloc[:, 0] if len(values.columns) > 0 else values.squeeze()
    return values

def delete_old_predictions():
    """Delete predictions for 2024 and 2025."""
    logger.info("🗑️  Deleting old predictions for 2024 and 2025...")
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='match_predictions'")
        if not cursor.fetchone():
            logger.warning("⚠️  Table 'match_predictions' does not exist. Skipping deletion.")
            return

        cursor.execute("DELETE FROM match_predictions WHERE year IN (2024, 2025)")
        rows_deleted = cursor.rowcount
        conn.commit()
        logger.info(f"✅ Deleted {rows_deleted} rows from match_predictions.")
    except Exception as e:
        logger.error(f"❌ Error deleting predictions: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def train_model():
    """Train and save the Random Forest model."""
    logger.info("🏋️  Starting model training...")
    try:
        # Load data
        logger.info("loading data...")
        X_train = pd.read_csv(PREPROCESSED_FEATURE_CSV, low_memory=False)
        y_train_df = pd.read_csv(PREPROCESSED_TARGET_CSV, low_memory=False)
        X_test = pd.read_csv(PREPROCESSED_TEST_FEATURE_CSV, low_memory=False)
        y_test_df = pd.read_csv(PREPROCESSED_TEST_TARGET_CSV, low_memory=False)
        
        y_train = _extract_series_or_array(y_train_df)
        y_test = _extract_series_or_array(y_test_df)
        
        # Train
        logger.info(f"Training on {len(X_train)} samples...")
        model = model_train(X_train, y_train, x_test=X_test, y_test=y_test)
        
        # Save
        os.makedirs(MODELS_PATH, exist_ok=True)
        with open(MODEL_FILE, 'wb') as f:
            pickle.dump(model, f)
        logger.info(f"✅ Model saved to {MODEL_FILE}")
        
    except Exception as e:
        logger.error(f"❌ Error during model training: {e}")
        raise

def run_predictions():
    """Generate predictions for 2024 and 2025."""
    logger.info("🔮 Generating weekly predictions...")
    try:
        # Generate for 2024
        logger.info("Processing 2024 season...")
        generate_weekly_predictions(2024)
        
        # Generate for 2025
        logger.info("Processing 2025 season...")
        generate_weekly_predictions(2025)
        
        logger.info("✅ Weekly predictions generated.")
    except Exception as e:
        logger.error(f"❌ Error generating predictions: {e}")
        raise

def update_future():
    """Update future predictions."""
    logger.info("📅 Updating future predictions...")
    try:
        update_future_predictions()
        logger.info("✅ Future predictions updated.")
    except Exception as e:
        logger.error(f"❌ Error updating future predictions: {e}")
        raise

if __name__ == "__main__":
    try:
        delete_old_predictions()
        train_model()
        run_predictions()
        update_future()
        logger.info("🎉 All updates completed successfully!")
    except Exception as e:
        logger.critical(f"❌ Script failed: {e}")
        sys.exit(1)
# sync 1774962763648406188
# sync 1774962858119474903
# sync 1774962858981352055
# sync 1774962859031856435
# sys_sync_5a12616b
# sys_sync_382829e8
