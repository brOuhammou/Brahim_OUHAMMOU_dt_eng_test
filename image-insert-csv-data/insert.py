#!/usr/bin/env python
"""
Data Insertion Service
This module handles the ETL process of loading CSV data into MySQL database.
"""

import csv
import time
from typing import Optional, Dict
import sqlalchemy
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.schema import Table

class DatabaseConnectionManager:
    """Handles database connection lifecycle and retries.
    """
    
    def __init__(self, connection_url: str, max_retries: int = 10, retry_delay: int = 5):
        self.connection_url = connection_url
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.engine: Optional[Engine] = None
        
    def connect(self) -> Engine:
        """Establishes database connection with retry mechanism."""
        self.engine = sqlalchemy.create_engine(self.connection_url)
        
        for attempt in range(self.max_retries):
            try:
                self.engine.connect()
                print("Connected to MySQL!")
                return self.engine
            except OperationalError as e:
                if attempt == self.max_retries - 1:
                    raise Exception(f"Could not connect to MySQL after {self.max_retries} attempts") from e
                print(f"MySQL not ready yet (attempt {attempt+1}/{self.max_retries})...")
                time.sleep(self.retry_delay)
                
        raise Exception("Unexpected connection failure")

class DataLoader:
    """Handles data loading operations.
    """
    
    def __init__(self, connection: Connection, metadata: sqlalchemy.MetaData):
        self.connection = connection
        self.metadata = metadata
        self.places_table = Table('places', metadata, autoload=True, autoload_with=connection.engine)
        self.people_table = Table('people', metadata, autoload=True, autoload_with=connection.engine)
        self.place_cache: Dict[str, int] = {}  # Cache for place lookups
        
    def _load_csv(self, file_path: str) -> csv.DictReader:
        """Opens and returns a CSV reader with proper error handling."""
        try:
            csv_file = open(file_path, 'r', encoding='utf-8')
            return csv.DictReader(csv_file)
        except (FileNotFoundError, IOError) as e:
            raise Exception(f"Failed to open CSV file {file_path}") from e

    def load_places(self, file_path: str) -> None:
        """Loads place data from CSV into database.
        """
        print("Inserting places data...")
        reader = self._load_csv(file_path)
        
        try:
            for row in reader:
                result = self.connection.execute(
                    self.places_table.insert().values(
                        city=row['city'],
                        county=row['county'],
                        country=row['country']
                    )
                )
                # Cache the place id for faster people insertion
                self.place_cache[row['city']] = result.inserted_primary_key[0]
        except SQLAlchemyError as e:
            raise Exception("Failed to insert places data") from e

    def load_people(self, file_path: str) -> None:
        """Loads people data from CSV into database.
        """
        print("Inserting people data...")
        reader = self._load_csv(file_path)
        
        try:
            for row in reader:
                # Use cached place_id instead of querying
                place_id = self.place_cache.get(row['place_of_birth'])
                if place_id is None:
                    raise ValueError(f"Place not found: {row['place_of_birth']}")
                
                self.connection.execute(
                    self.people_table.insert().values(
                        given_name=row['given_name'],
                        family_name=row['family_name'],
                        date_of_birth=row['date_of_birth'],
                        place_of_birth_id=place_id
                    )
                )
        except SQLAlchemyError as e:
            raise Exception("Failed to insert people data") from e

def main():
    """Main execution function."""
    try:
        # Initialize database connection
        db_manager = DatabaseConnectionManager(
            "mysql://user_dteng:cirilgroupt@database/db_dteng"
        )
        engine = db_manager.connect()
        
        # Create metadata
        metadata = sqlalchemy.MetaData()
        metadata.reflect(bind=engine)
        
        # Use connection context manager for proper cleanup
        with engine.connect() as connection:
            # Initialize data loader
            loader = DataLoader(connection, metadata)
            
            # Execute data loading pipeline
            loader.load_places('/data/places.csv')
            loader.load_people('/data/people.csv')
            
            print("Data inserted successfully!")
            
    except Exception as e:
        print(f"Error: {str(e)}")
        raise
    finally:
        if 'engine' in locals():
            engine.dispose()
            print("Connection disposed.")

if __name__ == "__main__":
    main()