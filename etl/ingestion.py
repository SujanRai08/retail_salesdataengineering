import os
import pandas as pd
from loguru import logger
from datetime import datetime
from dotenv import load_dotenv


os.makedirs("logs",exist_ok=True)
logger.add("logs/ingestion.logs",rotation="500 KB",level="INFO", format=" {name} - {level} - {message}")

load_dotenv()

class DataIngestion:
    """handles data ingestions"""
    def __init__(self,source_path=None):
        self.source_path = os.getenv("DATA_SOURCE_PATH")
        self.raw_data_dir = os.path.join("data", "raw")
        self.processed_data_dir = os.path.join("data", "processed")
        
        for dir_path in [self.raw_data_dir,self.processed_data_dir,"logs"]:
            os.makedirs(dir_path,exist_ok=True)

    def extract_data(self):
        """extract data fro the source csv file returns a pandas dataframe with the raw data"""
        try:
            logger.info(f"Starting data extraction from {self.source_path}")
            # check if file exists
            if not os.path.exists(self.source_path):
                logger.error(f"source file { self.source_path} does not exist")
                return None
            df = pd.read_csv(self.source_path)

            # log extration statistics
            record_count  = len(df)
            logger.info(f"successfully extracted {record_count} records")

            # create timestamp for the extraction
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            # saving the data with the timestamp
            raw_file_path = os.path.join(self.raw_data_dir,f"retail_sales_raw{timestamp}.csv")
            df.to_csv(raw_file_path,index=False)
            logger.info(f"raw data saved to {raw_file_path}")

            return df
        except Exception as e:
            logger.error(f"Error during data extraction: {str(e)}")
            raise
    def run_ingestions(self):
        """orchestrates the data ingestions process"""
        try:
            logger.info("Starting data ingestion process")
            # extract data from source
            df = self.extract_data()
            if df is None:
                logger.error("Data extraction failed. Aborting ingestion process..")
                return None
            logger.info(f"Data Ingestion completed successfully")
            return df
        except Exception as e:
            logger.error(f"Data ingestion process failed: {str(e)}")
            return None
    
if __name__ == "__main__":
    # Run ingestion as standalone script
    ingestion = DataIngestion()
    df = ingestion.run_ingestions()
    
    if df is not None:
        print(f"Ingestion completed successfully. Shape of data: {df.shape}")
    else:
        print("Ingestion failed. Check logs for details.")
