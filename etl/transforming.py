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

    def prepare_dimensional_data(self,df):
        """prepares data for star schema dimension model and return separate dataframes for each dimension
        and fact table"""
        try:
            logger.info("Prepare dimensional data")
            # create a dimension tables
            # customer dimension
            if all(col in df.columns for col in ["Customer ID","Customer Name"]):
                dim_customer = df[["Customer ID","Customer Name","Country","City","State","Postal Code","Region"]].drop_duplicates()
                dim_customer.columns = [col.lower().replace(" ","_") for col in dim_customer.columns]
            else:
                logger.warning("Customer dimension columns not found in DataFrame")
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
                # creating unidided date dimension with all uniquest date
                all_dates = pd.DataFrame()
                for col in date_columns:
                    dates = pd.DataFrame({
                        'date': pd.Series(df[col].unique()).dt.date
                    })
                    all_dates = pd.concat([all_dates,dates])
                dim_date = all_dates.drop_duplicates().reset_index(drop=True)
                # addign date attributes
                date_series = pd.to_datetime(dim_date['date'])
                dim_date['day'] = date_series.dt.day
                dim_date['month'] = date_series.dt.month
                dim_date['year'] = date_series.dt.year
                dim_date['quarter'] = date_series.dt.quarter
                dim_date['day_of_week'] = date_series.dt.dayofweek
                dim_date['is_weekend'] = dim_date['day_of_week'].apply(lambda x: 1 if x>= 5 else 0)

                # add date_id as auto_incrementing primary key
                dim_date['date_id'] = range(1,len(dim_date)+1)
            else:
                logger.warning("Dimension date columns not columns in dataframe")
                dim_date = None

            ## creating fact tables
            fact_columns = ["Row ID","Order ID","Customer ID","Product ID","Ship Mode","Sales","Quantity","Discount","Profit", "Profit Margin"]
            available_columns = [col for col in fact_columns if col in df.columns]
            fact_sales = df[available_columns].copy()
            # converting columns names to snake_caseee
            fact_sales.columns = [col.lower().replace(" ","_")for col in fact_sales.columns]
            #add date keys by joining with dim_date if available
            if dim_date is not None:
                # creating a mapping date dictionaries
                date_map = dict(zip(dim_date['date'],dim_date['date_id']))

                if "Order Date" in df.columns:
                    fact_sales['order_date_id'] = df['Order Date'].dt.date.map(date_map)
                if "Ship Date" in df.columns:
                    fact_sales['ship_date_id'] = df['Ship Date'].dt.date.map(date_map)
            logger.info("Dimensional data preparation completed succesfully.. ")

            return{
                'dim_customer': dim_customer,
                'dim_product': dim_product,
                'dim_date': dim_date,
                'fact_sales': fact_sales
            }
        except Exception as e:
            logger.error(f"Error during dimensional data preparation: ")

            raise

    def run_tranformation(self,df):
        """Orchestrates the data transformation process"""
        try:
            logger.info("Starting data tranformation workflow")
            #cleaned the data 
            cleaned_df = self.clean_data(df)

            # transformed the data
            transformed_df = self.transform_data(df)

            #prepare dimensional data
            dimensional_data = self.prepare_dimensional_data(df)

            #save tranformed data
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            # save the full tranformed datasets
            tranformed_path = os.path.join(self.processed_data_dir,f"retail_sales_tranformed_{timestamp}.csv")
            tranformed_path.to_csv(tranformed_path,index = False)
            logger.info(f"tranformed Data saved to {tranformed_path}")

            # save for dimensional tables
            for table_name,table_df in dimensional_data.items():
                if table_df is not None:
                    table_path = os.path.join(self.processed_data_dir,f"{table_name}_{timestamp}.csv")
                    table_df.to_csv(table_path,index=False)
                    logger.info(f"{table_name} saved to {table_path}")

            logger.info("Data tranformed workflow completed succesfully.")
            return dimensional_data
        except Exception as e:
            logger.error(f"Data tranformed workflow failed: {str(e)}")
            return None

if __name__ == "__main__":
    from ingestion import DataIngestion
    ingestion = DataIngestion()
    df = ingestion.run_ingestions()
    if df is not None:
        tranformation = DataTransformation()
        dimensional_data = tranformation.run_tranformation()
        if dimensional_data:
            for table_name,table_df in dimensional_data.items():
                if table_df is not None:
                    print(f"{table_name} shape: {table_df.shape}")
    else:
        print("Tranformation skipped due to failed ingestions")








                    

                        

                     




