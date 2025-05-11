import os
import logging
import pandas as pd
from sqlalchemy import inspect,text
from config.database import engine,Base,get_db
from models.schema import DimCustomer,DimDate,DimProduct,FactSales
from loguru import logger
from sqlalchemy.dialects.postgresql import insert

os.makedirs("logs",exist_ok=True)
logger.add("logs/ingestion.logs",rotation="500 KB",level="INFO", format=" {name} - {level} - {message}")

class DataLoading:
    """
    Handles loading transformed data into the PostgreSQL database
    """
    def __init__(self):
        self.engine = engine
        os.makedirs("logs", exist_ok=True)
    
    def create_tables(self):
        """
        Creates all tables defined in the SQLAlchemy models
        """
        try:
            logger.info("Creating database tables if they don't exist")
            
            # Create all tables defined in models
            Base.metadata.create_all(self.engine)
            
            # Get inspector to check table existence
            inspector = inspect(self.engine)
            tables = inspector.get_table_names()
            
            logger.info(f"Tables in database: {tables}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating database tables: {str(e)}")
            return False

    def load_dimension_table(self, df, table_model, key_column):
        """
        Loads data into a dimension table with conflict resolution (insert/update)
        """
        try:
            if df is None or df.empty:
                logger.warning(f"No data to load for {table_model.__tablename__}")
                return 0
            
            logger.info(f"Loading data into {table_model.__tablename__}")
            
            # Get a database session
            db = next(get_db())
            
            # Convert DataFrame to list of dictionaries
            records = df.to_dict('records')
            
            # Track counts
            insert_count = 0
            update_count = 0
            
            # Process each record
            for record in records:
                # Use upsert to handle conflict resolution
                stmt = insert(table_model).values(record)
                stmt = stmt.on_conflict_do_nothing(index_elements=[key_column])
                
                try:
                    db.execute(stmt)
                    insert_count += 1
                except Exception as e:
                    if 'psycopg2.errors.UniqueViolation' in str(e):
                        logger.warning(f"Duplicate record found for {key_column}: {record[key_column]}. Skipping insert.")
                    else:
                        raise e
            
            db.commit()
            logger.info(f"Loaded {table_model.__tablename__}: {insert_count} inserted, {update_count} updated")
            return insert_count + update_count
            
        except Exception as e:
            logger.error(f"Error loading {table_model.__tablename__}: {str(e)}")
            if 'db' in locals():
                db.rollback()  # Rollback the transaction in case of error
            return 0


    
    def load_fact_table(self, df):
        """
        Loads data into the fact table
        """
        try:
            if df is None or df.empty:
                logger.warning("No data to load for fact_sales")
                return 0
            
            logger.info("Loading data into fact_sales")
            
            # Get a database session
            db = next(get_db())
            
            # Convert DataFrame to list of dictionaries
            records = df.to_dict('records')
            
            # Process each record
            insert_count = 0
            for record in records:
                # Check if record already exists by row_id
                row_id = record.get('row_id')
                if row_id:
                    existing = db.query(FactSales).filter(FactSales.row_id == row_id).first()
                    
                    if existing:
                        # Skip existing records to avoid duplicates
                        continue
                
                # Create new fact record
                new_record = FactSales(**record)
                db.add(new_record)
                insert_count += 1
            
            # Commit changes
            db.commit()
            
            logger.info(f"Loaded fact_sales: {insert_count} inserted")
            return insert_count
            
        except Exception as e:
            logger.error(f"Error loading fact_sales: {str(e)}")
            if 'db' in locals():
                db.rollback()
            return 0
    
    def run_loading(self, dimensional_data):
        """
        Orchestrates the data loading process
        """
        try:
            logger.info("Starting data loading workflow")
            
            # Create tables if they don't exist
            if not self.create_tables():
                logger.error("Failed to create database tables. Aborting loading process.")
                return False
            
            # Load dimension tables first
            dim_tables = [
                (dimensional_data.get('dim_date'), DimDate, 'date_id'),
                (dimensional_data.get('dim_customer'), DimCustomer, 'customer_id'),
                (dimensional_data.get('dim_product'), DimProduct, 'product_id')
            ]
            
            for df, model, key_column in dim_tables:
                if df is not None:
                    self.load_dimension_table(df, model, key_column)
            
            # Load fact table last
            fact_df = dimensional_data.get('fact_sales')
            if fact_df is not None:
                self.load_fact_table(fact_df)
            
            logger.info("Data loading workflow completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Data loading workflow failed: {str(e)}")
            return False


if __name__ == "__main__":
    # For testing the loading module directly
    from ingestion import DataIngestion
    from transforming import DataTransformation
    
    # Run ingestion to get data
    ingestion = DataIngestion()
    df = ingestion.run_ingestion()
    
    if df is not None:
        # Run transformation
        transformation = DataTransformation()
        dimensional_data = transformation.run_transformation(df)
        
        if dimensional_data:
            # Run loading
            loading = DataLoading()
            success = loading.run_loading(dimensional_data)
            
            if success:
                print("Data loading completed successfully")
            else:
                print("Data loading failed")
    else:
        print("Loading skipped due to failed ingestion or transformation")