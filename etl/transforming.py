import pandas as pd
import numpy as np
from loguru import logger
import os
from datetime import datetime

os.makedirs("logs",exist_ok=True)
logger.add("logs/ingestion.logs",rotation="500 KB",level="INFO", format=" {name} - {level} - {message}")

class DataTransformation:
    """Handles cleaning and transformation of retail sales data"""
    def __init__(self):
        self.processed_data_dir = os.path.join("data","processed")
        os.makedirs(self.processed_data_dir,exist_ok=True)
        os.makedirs("logs",exist_ok=True)

    def clean_data(self,df):
        """cleans the data by handling missing values, duplicates, etc"""
        try:
            logger.info("Starting data cleaning process")
            initial_records = len(df)
            clean_df = df.copy()

            #checking missing values:
            missing_values = clean_df.isnull().sum().sum()
            if missing_values > 0:
                # for critical columns, we drops rows with missing values
                critical_columns = ["Order ID", "Product ID", "Customer ID", "Sales"]
                clean_df = clean_df.dropna(subset=critical_columns)
                # For non-critical columns, we fill missing values with appropriate defaults
                # Numeric columns with 0
                numeric_columns = clean_df.select_dtypes(include=["number"]).columns
                for col in numeric_columns:
                    if col not in critical_columns:
                        clean_df[col] - clean_df[col].fillna(0)
                string_columns = clean_df.select_dyptes(include = ["object"]).columns
                for col in string_columns:
                    if col not in critical_columns:
                        clean_df[col] = clean_df[col].fillna('Unknown')

            # handles duplicate
            duplicates = clean_df.duplicated().sum()
            logger.info(f"Found {duplicates} duplicate records")

            if duplicates > 0:
                clean_df = clean_df.drop_duplicates()
            date_columns = ["order Date", "Ship Date"]
            for col in date_columns:
                if col in clean_df.columns:
                    clean_df[col] = pd.to_datetime(clean_df[col])
            
            # log cleaning statistics
            final_records = len(clean_df)
            logger.info(f"Data Cleaning completed. Records: {initial_records} -> {final_records}")

            return clean_df
        except Exception as e:
            logger.error(f"Error data during data cleanning: {str(e)}")
            raise
        

