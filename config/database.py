from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base 
from sqlalchemy.orm import sessionmaker
from urllib.parse import quote_plus
from loguru import logger
import os

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = quote_plus(os.getenv("DB_PASSWORD"))  # encode special characters
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "retail_db")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Create engine and session factory
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_db():
    """Yields a database session and ensures proper cleanup."""
    db = SessionLocal()
    try:
        logger.info("Yielding database session")
        yield db
    except Exception as e:
        logger.exception("Error during DB session")
    finally:
        db.close()
        logger.info("Database session closed")
