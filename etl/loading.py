import os
import logging
import pandas as pd
from sqlalchemy import inspect,text
from config.database import engine,Base,get_db
from models.schema import DimCustomer,DimDate,DimProduct,FactSales
from loguru import logger

os.makedirs("logs",exist_ok=True)
logger.add("logs/ingestion.logs",rotation="500 KB",level="INFO", format=" {name} - {level} - {message}")

class DataLoading:
    """handles loading tranformed data into the postgres database"""
    def __init__(self):
        self.engine = engine
    def create_tables(self):
        """creates all tables defined in the sqlalchemy models"""
        try:
            logger.info("Creating database tables if they don't exits")

            #create all tables defined in models
            Base.metadata.create_all(self.engine)

            #get inspector to chect tables existence
            inspector = inspect(self.engine)
            tables = inspector.get_table_names()
            logger.info(f"tables in database: {tables}")

            return True
        except Exception as e:
            logger.error(f"Error creating database schema tables: {str(e)}")
            return False
    
    def load_dimension_tables(self,df,table_model,key_column):
        """load data into a dimension tables with conflict resolution"""
        try:
            if df is None or df.empty:
                logger.warning(f"No data to load for {table_model.__tablename__}")
                return 0
            logger.info(f"Loading the data into {table_model.__tablename__}")

            # get a database session
            db = next(get_db())

            #converting dataframe to list of dictionarues
            records = df.to_dict("records")

            # track counts
            insert_count = 0
            update_count = 0

            # process each records
            for record in records:
                # check if records already exists
                key_value = record[key_column]
                exisiting = db.query(table_model).filter(getattr(table_model,key_column) == key_value).first()

                if exisiting:
                    # update existing records
                    for key,value in record.items():
                        if hasattr(exisiting,key):
                            setattr(exisiting,key,value)
                    update_count += 1
                else:
                    # inset new records:
                    new_record = table_model(**record)
                    db.add(new_record)
                    insert_count +=1

            db.commit()

            logger.info(f"Loaded {table_model.__tablename__}: {insert_count} inserted, {update_count} updated")
            return insert_count + update_count
            
        except Exception as e:
            logger.error(f"Error loading {table_model.__tablename__}: {str(e)}")
            if 'db' in locals():
                db.rollback()
            return 0

    def load_fact_table(self,df):
        """loads data into the fact tables"""
        try:
            if df is None or df.empty:
                logger.warning("No data to load for fact_sales")
                return 0
            
            logger.info("Loading data into fact_sales")
            
            # Get a database session
            db = next(get_db())
            
            # Convert DataFrame to list of dictionaries
            records = df.to_dict('records')

            # process each records

            insert_count = 0
            for record in records:
                #checknng if record already exist by row_id
                row_id = record.get("row_id")
                if row_id:
                    existing = db.query(FactSales).filter(FactSales.row_id == row_id).first()

                    if existing:
                        # skipping exisitng records to avoid any duplicates
                        continue
                new_record = FactSales(**record)
                db.add(new_record)
                insert_count += 1
            db.commit()
            logger.info(f"Loaded fact_sales: {insert_count} inserted")
            return insert_count
        except Exception as e:
            logger.error(f"Error loading fact_sales: {str(e)}")
            if 'db' in locals():
                db.rollback()
            return 0
        
    def run_loading(self,dimensional_data):
        """orchestrates the data loading process"""
        try:
            logger.info("Starting data loading workflow")
            
            # Create tables if they don't exist
            if not self.create_tables():
                logger.error("Failed to create database tables. Aborting loading process.")

                return False
            
            # loading dimesnion tables first
            dim_tables = [
                (dimensional_data.get('dim_date'), DimDate, 'date_id'),
                (dimensional_data.get('dim_customer'), DimCustomer, 'customer_id'),
                (dimensional_data.get('dim_product'), DimProduct, 'product_id')
            ]
            for df,model,key_column in dim_tables:
                if df is not None:
                    self.load_dimension_tables(df,model,key_column)

            fact_df = dimensional_data.get('fact_sales')
            if fact_df is not None:
                self.load_fact_table(fact_df)
            logger.info("Data loading workflow completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Data loading workflow failed: {str(e)}")
            return False
        

if __name__ == "__main__":
    # for testing the loading module directly: 
    from ingestion import DataIngestion
    from transforming import DataTransformation

    # run ingestions to get data
    ingestion = DataIngestion()
    df = ingestion.run_ingestions()
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



        



