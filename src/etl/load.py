"""
Data loading module.
Handles loading data to Google Sheets (3 worksheets) and Data Lake (historical backup).
"""

import pandas as pd
from datetime import datetime
from typing import Dict

from src.services import DriveService
from src.config import settings


class DataLoader:
    """
    Handles data loading operations to Google Sheets and Data Lake.
    Writes to 3 separate worksheets in MASTER_DATA_CLEAN.
    """
    
    def __init__(self, drive_service: DriveService):
        """
        Initialize data loader.
        
        Args:
            drive_service: Authenticated Drive service instance
        """
        self.drive_service = drive_service
    
    def load_all_data(self, transformed_data: Dict[str, pd.DataFrame], execution_date: datetime) -> None:
        """
        Load all transformed data to Google Sheets and Data Lake.
        
        Args:
            transformed_data: Dictionary with "keywords", "installs", "users" DataFrames
            execution_date: Date of pipeline execution
            
        Raises:
            Exception: If any load operation fails
        """
        print("\n[LOAD] Starting data load operations...")
        
        # Load to Google Sheets (3 worksheets in MASTER_DATA_CLEAN)
        self._load_to_master_sheets(transformed_data)
        
        # Load to Data Lake (historical backup as CSV)
        self._load_to_data_lake(transformed_data, execution_date)
        
        print("[LOAD] All data successfully loaded")
    
    def _load_to_master_sheets(self, data: Dict[str, pd.DataFrame]) -> None:
        """
        Load data to MASTER_DATA_CLEAN Google Sheet (3 separate worksheets).
        Clears existing content and writes new data.
        
        Args:
            data: Dictionary with "keywords", "installs", "users" DataFrames
            
        Raises:
            Exception: If update fails
        """
        try:
            print("[LOAD] Writing to MASTER_DATA_CLEAN Google Sheet...")
            
            # Find the MASTER_DATA_CLEAN sheet
            master_file_id = self.drive_service.find_file_by_name(settings.MASTER_DATA_SHEET)
            
            if not master_file_id:
                raise Exception(f"Sheet '{settings.MASTER_DATA_SHEET}' not found")
            
            spreadsheet = self.drive_service.gspread_client.open_by_key(master_file_id)
            
            # Load each data type to its respective worksheet
            self._update_worksheet(spreadsheet, settings.KEYWORDS_SHEET, data["keywords"])
            self._update_worksheet(spreadsheet, settings.INSTALLS_SHEET, data["installs"])
            self._update_worksheet(spreadsheet, settings.USERS_SHEET, data["users"])
            
            print(f"[LOAD] Successfully updated {settings.KEYWORDS_SHEET}: {len(data['keywords'])} rows")
            print(f"[LOAD] Successfully updated {settings.INSTALLS_SHEET}: {len(data['installs'])} rows")
            print(f"[LOAD] Successfully updated {settings.USERS_SHEET}: {len(data['users'])} rows")
            
        except Exception as e:
            raise Exception(f"Failed to load data to master sheets: {str(e)}")
    
    def _update_worksheet(self, spreadsheet, worksheet_name: str, df: pd.DataFrame) -> None:
        """
        Update a specific worksheet in the spreadsheet.
        Creates worksheet if it doesn't exist.
        
        Args:
            spreadsheet: gspread Spreadsheet object
            worksheet_name: Name of the worksheet to update
            df: DataFrame to write
        """
        try:
            # Try to get existing worksheet
            try:
                worksheet = spreadsheet.worksheet(worksheet_name)
            except:
                # Create new worksheet if it doesn't exist
                worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=1000, cols=20)
            
            # Clear existing content
            worksheet.clear()
            
            # Date column is already formatted as DD/MM/YYYY string in transformation
            # No need to convert datetime columns here
            df_copy = df.copy()
            
            # Replace inf/-inf/nan with None for JSON compatibility
            df_copy = df_copy.replace([float('inf'), float('-inf')], None)
            df_copy = df_copy.fillna('')
            
            # Prepare data with headers
            data = [df_copy.columns.tolist()] + df_copy.values.tolist()
            
            # Update worksheet
            worksheet.update(data, value_input_option="RAW")
            
        except Exception as e:
            raise Exception(f"Failed to update worksheet '{worksheet_name}': {str(e)}")
    
    def _load_to_data_lake(self, data: Dict[str, pd.DataFrame], execution_date: datetime) -> None:
        """
        Save all DataFrames as CSV files for historical backup locally.
        Controlled by RUN_BACKUP flag (cell B4 in Panel de Control).
        
        Args:
            data: Dictionary with "keywords", "installs", "users" DataFrames
            execution_date: Date to determine filename
            
        Raises:
            Exception: If save fails
        """
        # Check if backup is enabled (B4 in Panel de Control)
        if not settings.RUN_BACKUP:
            print("[LOAD] Backup disabled (Panel B4 = FALSE). Skipping local data lake save.")
            return
        
        try:
            print("[LOAD] Saving historical backup to local data lake...")
            
            # Create processed data directory
            import os
            processed_dir = "data/processed"
            os.makedirs(processed_dir, exist_ok=True)
            
            # Save each data type as separate CSV file
            date_str = execution_date.strftime('%Y%m%d')
            for data_type, df in data.items():
                filename = f"{data_type}_{date_str}.csv"
                filepath = os.path.join(processed_dir, filename)
                df.to_csv(filepath, index=False)
                print(f"[LOAD] Saved {filename} ({len(df)} rows)")
            
            print(f"[LOAD] Historical backup saved to {processed_dir}/")
            
        except Exception as e:
            raise Exception(f"Failed to save local backup: {str(e)}")

