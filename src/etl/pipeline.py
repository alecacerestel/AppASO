"""
ETL Pipeline orchestration module.
Coordinates the Extract, Transform, Load process for ASO data.
"""

from datetime import datetime

from src.etl.extract import DataExtractor
from src.etl.transform import DataTransformer
from src.etl.load import DataLoader
from src.services import DriveService


class ETLPipeline:
    """
    Orchestrates the complete ETL pipeline execution.
    Processes keywords, installs, and active users data from Apple and Google.
    """
    
    def __init__(self, drive_service: DriveService):
        """
        Initialize ETL pipeline with required services.
        
        Args:
            drive_service: Authenticated Drive service instance
        """
        self.drive_service = drive_service
        self.extractor = DataExtractor(drive_service.drive_service)
        self.transformer = DataTransformer()
        self.loader = DataLoader(drive_service)
    
    def run(self) -> None:
        """
        Execute the complete ETL pipeline.
        
        Raises:
            Exception: If any step of the pipeline fails
        """
        execution_date = datetime.now()
        
        print("="*70)
        print("AppASO ETL Pipeline - ASO Data Processing")
        print("="*70)
        print(f"Execution started: {execution_date.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Step 1: Extract data from Google Drive RAW folder
        print("\n[STEP 1/3] EXTRACTION")
        print("-" * 70)
        raw_data = self.extractor.extract_all_data()
        
        # Step 2: Transform data (standardize, clean, add business logic)
        print("\n[STEP 2/3] TRANSFORMATION")
        print("-" * 70)
        transformed_data = self.transformer.transform_all_data(raw_data)
        
        # Step 3: Load data to Google Sheets and Data Lake
        print("\n[STEP 3/3] LOAD")
        print("-" * 70)
        self.loader.load_all_data(transformed_data, execution_date)
        
        print("\n" + "="*70)
        print("ETL Pipeline execution completed successfully")
        print("="*70)
        print(f"Execution finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

