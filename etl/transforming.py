import pandas as pd
import numpy as np
from loguru import logger
import os
from datetime import datetime

os.makedirs("logs",exist_ok=True)
logger.add("logs/ingestion.logs",rotation="500 KB",level="INFO", format=" {name} - {level} - {message}")

class DataTransformation:
    """
    Handles cleaning and transformation of retail sales data
    """
    def __init__(self):
        self.processed_data_dir = os.path.join("data", "processed")
        os.makedirs(self.processed_data_dir, exist_ok=True)
        os.makedirs("logs", exist_ok=True)
    
    def clean_data(self, df):
        """
        Cleans the data by handling missing values, duplicates, etc.
        """
        try:
            logger.info("Starting data cleaning process")
            initial_records = len(df)
            
            # Make a copy to avoid modifying the original dataframe
            clean_df = df.copy()
            
            # Check for missing values
            missing_values = clean_df.isnull().sum().sum()
            logger.info(f"Found {missing_values} missing values")
            
            # Handle missing values
            if missing_values > 0:
                # For critical columns, we drop rows with missing values
                critical_columns = ["Order ID", "Product ID", "Customer ID", "Sales"]
                clean_df = clean_df.dropna(subset=critical_columns)
                
                # For non-critical columns, we fill missing values with appropriate defaults
                # Numeric columns with 0
                numeric_columns = clean_df.select_dtypes(include=['number']).columns
                for col in numeric_columns:
                    if col not in critical_columns:
                        clean_df[col] = clean_df[col].fillna(0)
                
                # String columns with 'Unknown'
                string_columns = clean_df.select_dtypes(include=['object']).columns
                for col in string_columns:
                    if col not in critical_columns:
                        clean_df[col] = clean_df[col].fillna('Unknown')
            
            # Handle duplicates
            duplicates = clean_df.duplicated().sum()
            logger.info(f"Found {duplicates} duplicate records")
            
            if duplicates > 0:
                clean_df = clean_df.drop_duplicates()
            
            # Convert date columns to datetime
            date_columns = ["Order Date", "Ship Date"]
            for col in date_columns:
                if col in clean_df.columns:
                    clean_df[col] = pd.to_datetime(clean_df[col], dayfirst=True, errors='coerce')
            
            # Log cleaning statistics
            final_records = len(clean_df)
            logger.info(f"Data cleaning completed. Records: {initial_records} -> {final_records}")
            
            return clean_df
            
        except Exception as e:
            logger.error(f"Error during data cleaning: {str(e)}")
            raise
    
    def transform_data(self, df):
        """
        Transforms the data by creating derived features, normalizing, etc.
        """
        try:
            logger.info("Starting data transformation process")
            
            # Make a copy to avoid modifying the original dataframe
            transform_df = df.copy()
            
            # Extract date components for date dimension
            if "Order Date" in transform_df.columns:
                transform_df["Order Year"] = transform_df["Order Date"].dt.year
                transform_df["Order Month"] = transform_df["Order Date"].dt.month
                transform_df["Order Day"] = transform_df["Order Date"].dt.day
                transform_df["Order Quarter"] = transform_df["Order Date"].dt.quarter
                transform_df["Order Day of Week"] = transform_df["Order Date"].dt.dayofweek
                transform_df["Order Is Weekend"] = transform_df["Order Day of Week"].apply(lambda x: 1 if x >= 5 else 0)
            
            if "Ship Date" in transform_df.columns:
                transform_df["Ship Year"] = transform_df["Ship Date"].dt.year
                transform_df["Ship Month"] = transform_df["Ship Date"].dt.month
                transform_df["Ship Day"] = transform_df["Ship Date"].dt.day
            
            # Calculate shipping duration in days
            if "Order Date" in transform_df.columns and "Ship Date" in transform_df.columns:
                transform_df["Shipping Duration"] = (transform_df["Ship Date"] - transform_df["Order Date"]).dt.days
            
            # Calculate profit if sales column exists (assuming standard 20% margin if profit not available)
            if "Sales" in transform_df.columns and "Profit" not in transform_df.columns:
                transform_df["Profit"] = transform_df["Sales"] * 0.2
            
            # Calculate profit margin
            if "Sales" in transform_df.columns and "Profit" in transform_df.columns:
                # Avoid division by zero
                transform_df["Profit Margin"] = np.where(
                    transform_df["Sales"] > 0,
                    transform_df["Profit"] / transform_df["Sales"],
                    0
                )
            
            # Add a default quantity of 1 if not present
            if "Quantity" not in transform_df.columns:
                transform_df["Quantity"] = 1
            
            # Add a default discount of 0 if not present
            if "Discount" not in transform_df.columns:
                transform_df["Discount"] = 0.0
            
            logger.info("Data transformation completed successfully")
            return transform_df
            
        except Exception as e:
            logger.error(f"Error during data transformation: {str(e)}")
            raise
    
    def prepare_dimensional_data(self, df):
        """
        Prepares data for star schema dimensional model
        Returns separate dataframes for each dimension and fact table
        """
        try:
            logger.info("Preparing dimensional data")
            
            # Create dimension tables
            
            # Customer dimension
            if all(col in df.columns for col in ["Customer ID", "Customer Name"]):
                dim_customer = df[["Customer ID", "Customer Name", "Segment", 
                                  "Country", "City", "State", 
                                  "Postal Code", "Region"]].drop_duplicates()
                dim_customer.columns = [col.lower().replace(" ", "_") for col in dim_customer.columns]
            else:
                logger.warning("Customer dimension columns not found in dataframe")
                dim_customer = None
            
            # Product dimension
            if all(col in df.columns for col in ["Product ID", "Product Name"]):
                dim_product = df[["Product ID", "Category", "Sub-Category", "Product Name"]].drop_duplicates()
                dim_product.columns = [col.lower().replace(" ", "_").replace("-", "_") for col in dim_product.columns]
            else:
                logger.warning("Product dimension columns not found in dataframe")
                dim_product = None
            
            # Date dimension (from both Order Date and Ship Date)
            date_columns = []
            if "Order Date" in df.columns:
                date_columns.append("Order Date")
            if "Ship Date" in df.columns:
                date_columns.append("Ship Date")
            
            if date_columns:
                # Create unified date dimension with all unique dates
                all_dates = pd.DataFrame()
                for col in date_columns:
                    dates = pd.DataFrame({
                        'date': pd.Series(df[col].unique()).dt.date
                    })
                    all_dates = pd.concat([all_dates, dates])
                
                dim_date = all_dates.drop_duplicates().reset_index(drop=True)
                
                # Add date attributes
                date_series = pd.to_datetime(dim_date['date'])
                dim_date['day'] = date_series.dt.day
                dim_date['month'] = date_series.dt.month
                dim_date['year'] = date_series.dt.year
                dim_date['quarter'] = date_series.dt.quarter
                dim_date['day_of_week'] = date_series.dt.dayofweek
                dim_date['is_weekend'] = dim_date['day_of_week'].apply(lambda x: 1 if x >= 5 else 0)
                
                # Add date_id as auto-incrementing primary key
                dim_date['date_id'] = range(1, len(dim_date) + 1)
            else:
                logger.warning("Date dimension columns not found in dataframe")
                dim_date = None
            
            # Create fact table
            fact_columns = ["Row ID", "Order ID", "Customer ID", "Product ID", 
                           "Ship Mode", "Sales", "Quantity", "Discount", "Profit", "Profit Margin"]
            
            available_columns = [col for col in fact_columns if col in df.columns]
            fact_sales = df[available_columns].copy()
            
            # Convert column names to snake_case
            fact_sales.columns = [col.lower().replace(" ", "_") for col in fact_sales.columns]
            
            # Add date keys by joining with dim_date if available
            if dim_date is not None:
                # Create date mapping dictionaries
                date_map = dict(zip(dim_date['date'], dim_date['date_id']))
                
                # Map order_date to order_date_id
                if "Order Date" in df.columns:
                    fact_sales['order_date_id'] = df["Order Date"].dt.date.map(date_map)
                
                # Map ship_date to ship_date_id
                if "Ship Date" in df.columns:
                    fact_sales['ship_date_id'] = df["Ship Date"].dt.date.map(date_map)
            
            logger.info("Dimensional data preparation completed successfully")
            
            return {
                'dim_customer': dim_customer,
                'dim_product': dim_product,
                'dim_date': dim_date,
                'fact_sales': fact_sales
            }
            
        except Exception as e:
            logger.error(f"Error during dimensional data preparation: {str(e)}")
            raise
    
    def run_transformation(self, df):
        """
        Orchestrates the data transformation process
        """
        try:
            logger.info("Starting data transformation workflow")
            
            # Clean the data
            cleaned_df = self.clean_data(df)
            
            # Transform the data
            transformed_df = self.transform_data(cleaned_df)
            
            # Prepare dimensional data
            dimensional_data = self.prepare_dimensional_data(transformed_df)
            
            # Save transformed data
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Save the full transformed dataset
            transformed_path = os.path.join(self.processed_data_dir, f"retail_sales_transformed_{timestamp}.csv")
            transformed_df.to_csv(transformed_path, index=False)
            logger.info(f"Transformed data saved to {transformed_path}")
            
            # Save dimensional tables
            for table_name, table_df in dimensional_data.items():
                if table_df is not None:
                    table_path = os.path.join(self.processed_data_dir, f"{table_name}_{timestamp}.csv")
                    table_df.to_csv(table_path, index=False)
                    logger.info(f"{table_name} saved to {table_path}")
            
            logger.info("Data transformation workflow completed successfully")
            return dimensional_data
            
        except Exception as e:
            logger.error(f"Data transformation workflow failed: {str(e)}")
            return None


if __name__ == "__main__":
    # For testing the transformation module directly
    from ingestion import DataIngestion
    
    # Run ingestion to get data
    ingestion = DataIngestion()
    df = ingestion.run_ingestion()
    
    if df is not None:
        # Run transformation
        transformation = DataTransformation()
        dimensional_data = transformation.run_transformation(df)
        
        if dimensional_data:
            for table_name, table_df in dimensional_data.items():
                if table_df is not None:
                    print(f"{table_name} shape: {table_df.shape}")
    else:
        print("Transformation skipped due to failed ingestion")