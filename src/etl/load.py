"""
Data loading module.
Handles loading data to Google Drive and Google Sheets.
"""

import pandas as pd
from datetime import datetime

from src.services import DriveService


class DataLoader:
    """
    Handles data loading operations to Google Drive and Sheets.
    """
    
    def __init__(self, drive_service: DriveService):
        """
        Initialize data loader.
        
        Args:
            drive_service: Authenticated Drive service instance
        """
        self.drive_service = drive_service
    
    def load_data(self, df: pd.DataFrame, execution_date: datetime) -> None:
        """
        Load transformed data to both Data Lake (historic) and Data Warehouse (current).
        
        Args:
            df: Transformed DataFrame to load
            execution_date: Date of pipeline execution
            
        Raises:
            Exception: If any load operation fails
        """
        # Load to Data Lake (historic CSV)
        self._load_to_data_lake(df, execution_date)
        
        # Load to Data Warehouse (current master sheet)
        self._load_to_data_warehouse(df)
    
    def _load_to_data_lake(self, df: pd.DataFrame, execution_date: datetime) -> None:
        """
        Save DataFrame to Data Lake as CSV file.
        
        Args:
            df: DataFrame to save
            execution_date: Date to determine folder structure
            
        Raises:
            Exception: If save fails
        """
        try:
            self.drive_service.save_to_data_lake(df, execution_date)
            print(f"Data successfully saved to Data Lake for {execution_date.strftime('%Y-%m-%d')}")
        except Exception as e:
            raise Exception(f"Failed to load data to Data Lake: {str(e)}")
    
    def _load_to_data_warehouse(self, df: pd.DataFrame) -> None:
        """
        Update the master data sheet in Data Warehouse.
        
        Args:
            df: DataFrame to upload
            
        Raises:
            Exception: If update fails
        """
        try:
            self.drive_service.update_master_data(df)
            print("Data successfully updated in Data Warehouse (MASTER_DATA_CLEAN)")
        except Exception as e:
            raise Exception(f"Failed to load data to Data Warehouse: {str(e)}")
