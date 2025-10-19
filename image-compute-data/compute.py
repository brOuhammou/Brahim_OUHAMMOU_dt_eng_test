#!/usr/bin/env python
"""
Data Computation Service

This module handles the computation of population statistics by country and outputs
the results in JSON format.

"""

import json
import time
from typing import Dict, Any, Optional
from pathlib import Path
import sqlalchemy
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.sql import select, func
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

class PopulationAnalyzer:
    """Handles population analysis and statistics computation.
    """
    
    def __init__(self, connection: Connection, metadata: sqlalchemy.MetaData):
        self.connection = connection
        self.metadata = metadata
        self.places_table = Table('places', metadata, autoload=True, autoload_with=connection.engine)
        self.people_table = Table('people', metadata, autoload=True, autoload_with=connection.engine)
        
    def compute_population_by_country(self) -> Dict[str, int]:
        """Computes population statistics by country.
        
        Returns:
            Dict[str, int]: Dictionary with country names as keys and population counts as values
        """
        try:
            # Build optimized query using SQL functions
            query = select([
                self.places_table.c.country,
                func.count(self.people_table.c.id).label('population')
            ]).select_from(
                self.people_table.join(
                    self.places_table,
                    self.people_table.c.place_of_birth_id == self.places_table.c.id
                )
            ).group_by(self.places_table.c.country)
            
            # Execute query and format results
            rows = self.connection.execute(query).fetchall()
            return {row[0]: row[1] for row in rows}
            
        except SQLAlchemyError as e:
            raise Exception("Failed to compute population statistics") from e

class JSONOutputWriter:
    """Handles JSON file output operations.
    """
    
    @staticmethod
    def write_json(data: Dict[str, Any], output_path: str) -> None:
        """Writes data to JSON file.
        
        Args:
            data: Dictionary to be written to JSON
            output_path: Path to output JSON file
        """
        try:
            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w', encoding='utf-8') as json_file:
                json.dump(data, json_file, separators=(',', ':'))
                
        except (IOError, json.JSONDecodeError) as e:
            raise Exception(f"Failed to write JSON to {output_path}") from e

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
            # Initialize analyzer and compute statistics
            analyzer = PopulationAnalyzer(connection, metadata)
            population_stats = analyzer.compute_population_by_country()
            
            # Write results to JSON file
            writer = JSONOutputWriter()
            writer.write_json(population_stats, '/data/summary_output.json')
            
            print("Data computed and written to summary_output.json successfully!")
            
    except Exception as e:
        print(f"Error: {str(e)}")
        raise
    finally:
        if 'engine' in locals():
            engine.dispose()
            print("Connection disposed.")

if __name__ == "__main__":
    main()