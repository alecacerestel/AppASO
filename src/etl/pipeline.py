"""
ETL Pipeline orchestration module.
Coordinates the Extract, Transform, Load process.
"""

from datetime import datetime

from src.etl.extract import DataExtractor
from src.etl.transform import DataTransformer
from src.etl.load import DataLoader
from src.services import DriveService


class ETLPipeline:
    """
    Orchestrates the complete ETL pipeline execution.
    """
    
    def __init__(self, drive_service: DriveService):
        """
        Initialize ETL pipeline with required services.
        
        Args:
            drive_service: Authenticated Drive service instance
        """
        self.drive_service = drive_service
        self.extractor = DataExtractor()
        self.transformer = DataTransformer()
        self.loader = DataLoader(drive_service)
    
    def run(self) -> None:
        """
        Execute the complete ETL pipeline.
        
        Raises:
            Exception: If any step of the pipeline fails
        """
        execution_date = datetime.now()
        
        print("Starting ETL Pipeline execution...")
        print(f"Execution date: {execution_date.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Step 1: Extract
        print("\n[1/3] Extracting data...")
        raw_data = self.extractor.extract_data()
        print(f"Extracted {len(raw_data)} records")
        
        # Step 2: Transform
        print("\n[2/3] Transforming data...")
        transformed_data = self.transformer.transform_data(raw_data)
        print(f"Transformation complete. {len(transformed_data)} clean records")
        
        # Step 3: Load
        print("\n[3/3] Loading data...")
        self.loader.load_data(transformed_data, execution_date)
        
        print("\nETL Pipeline execution completed successfully!")
