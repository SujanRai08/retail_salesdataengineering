import os
import logging
import time
import schedule
from datetime import datetime
from dotenv import load_dotenv
from loguru import logger
# Import ETL modules
from etl.ingestion import DataIngestion
from etl.transforming import DataTransformation
from etl.loading import DataLoading

# Load environment variables
load_dotenv()

# Create necessary directories

os.makedirs("logs", exist_ok=True)
os.makedirs("data/raw", exist_ok=True)
os.makedirs("data/processed", exist_ok=True)

# Add logger to create and rotate logs
logger.add("logs/pipeline.logs", rotation="500 KB", level="INFO", format="{time} - {name} - {level} = {message}")

def run_pipeline():
    """
    Executes the complete ETL pipeline
    """
    start_time = time.time()
    logger.info("Starting ETL pipeline execution")
    
    # Step 1: Ingestion
    ingestion = DataIngestion()
    df = ingestion.run_ingestion()
    
    if df is None:
        logger.error("Pipeline failed at ingestion step")
        return False
    
    # Step 2: Transformation
    transformation = DataTransformation()
    dimensional_data = transformation.run_transformation(df)
    
    if dimensional_data is None:
        logger.error("Pipeline failed at transformation step")
        return False
    
    # Step 3: Loading
    loading = DataLoading()
    success = loading.run_loading(dimensional_data)
    
    if not success:
        logger.error("Pipeline failed at loading step")
        return False
    
    # Calculate execution time
    execution_time = time.time() - start_time
    logger.info(f"Pipeline executed successfully in {execution_time:.2f} seconds")
    
    return True


def schedule_pipeline():
    """
    Schedules the pipeline to run at the specified interval
    """
    # Get interval from environment (default to 60 minutes)
    interval_minutes = int(os.getenv("INGESTION_INTERVAL", "60"))
    
    logger.info(f"Scheduling pipeline to run every {interval_minutes} minutes")
    
    # Schedule the pipeline
    schedule.every(interval_minutes).minutes.do(run_pipeline)
    
    # Run once immediately
    logger.info("Running pipeline immediately for initial load")
    run_pipeline()
    
    # Keep the script running to execute scheduled jobs
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    logger.info("ETL Pipeline Scheduler starting")
    
    try:
        schedule_pipeline()
    except KeyboardInterrupt:
        logger.info("Pipeline scheduler stopped by user")
    except Exception as e:
        logger.error(f"Pipeline scheduler failed with error: {str(e)}")