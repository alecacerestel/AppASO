"""
Data loading module.
Handles loading data to Google Sheets (3 worksheets) and Data Lake (historical backup).
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Dict
import os

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
        
        # Load to Data Lake - Local (historical backup as CSV)
        self._load_to_data_lake(transformed_data, execution_date)
        
        # Load to Data Lake - Google Drive (partitioned structure)
        self._load_to_data_lake_drive(transformed_data, execution_date)
        
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
    
    def _load_to_data_lake_drive(self, data: Dict[str, pd.DataFrame], execution_date: datetime) -> None:
        """
        Upload current day's data to Data_Lake_Historic Google Sheet as new worksheets.
        Reads data directly from MASTER_DATA_CLEAN (Data Warehouse) to ensure consistency.
        
        NEW Strategy: Always backup current execution's data from Data Warehouse.
        - Works regardless of execution environment (GitHub Actions or local)
        - No dependency on local CSV files
        - Always creates backup from authoritative source (MASTER_DATA_CLEAN)
        
        Each day creates 3 new worksheets: YYYYMMDD_keywords, YYYYMMDD_installs, YYYYMMDD_users
        
        Controlled by RUN_BACKUP_DRIVE flag.
        
        Args:
            data: Dictionary with current day's DataFrames (used for backup)
            execution_date: Current date (D), will backup data from this execution
            
        Raises:
            Exception: If upload fails
        """
        # Check if Drive backup is enabled
        if not settings.RUN_BACKUP_DRIVE:
            print("[LOAD] Drive backup disabled. Skipping.")
            return
        
        try:
            date_str = execution_date.strftime('%Y%m%d')
            
            print(f"[LOAD] Creating backup in Data_Lake_Historic for {execution_date.strftime('%Y-%m-%d')}...")
            
            # Open the Data_Lake_Historic Google Sheet
            spreadsheet = self.drive_service.gspread_client.open_by_key(settings.DATA_LAKE_HISTORIC_SHEET_ID)
            
            # Backup each data type using the processed data from this execution
            data_types = {
                "keywords": data["keywords"],
                "installs": data["installs"],
                "users": data["users"]
            }
            
            for data_type, df in data_types.items():
                # Create worksheet name: YYYYMMDD_datatype (e.g., "20251211_keywords")
                worksheet_name = f"{date_str}_{data_type}"
                
                # Upload to worksheet
                self._add_worksheet_from_csv(spreadsheet, worksheet_name, df)
                print(f"[LOAD] ✅ Added worksheet '{worksheet_name}' ({len(df)} rows)")
            
            print(f"[LOAD] Drive backup completed: 3 worksheets added to Data_Lake_Historic")
            
        except Exception as e:
            raise Exception(f"Failed to save Drive backup: {str(e)}")
    
    def _add_worksheet_from_csv(self, spreadsheet, worksheet_name: str, df: pd.DataFrame) -> None:
        """
        Add a new worksheet to an existing Google Sheet from a DataFrame.
        If worksheet already exists, it will be replaced.
        
        Args:
            spreadsheet: gspread Spreadsheet object
            worksheet_name: Name for the new worksheet
            df: DataFrame to upload
            
        Raises:
            Exception: If operation fails
        """
        try:
            # Sanitize DataFrame
            df_copy = df.copy()
            df_copy = df_copy.replace([float('inf'), float('-inf')], None)
            df_copy = df_copy.fillna('')
            
            # Check if worksheet already exists
            try:
                existing_ws = spreadsheet.worksheet(worksheet_name)
                # Delete existing worksheet
                spreadsheet.del_worksheet(existing_ws)
                print(f"[LOAD]    Deleted existing worksheet: {worksheet_name}")
            except:
                # Worksheet doesn't exist, that's fine
                pass
            
            # Create new worksheet
            worksheet = spreadsheet.add_worksheet(
                title=worksheet_name,
                rows=len(df_copy) + 1,
                cols=len(df_copy.columns)
            )
            
            # Prepare data (headers + rows)
            headers = df_copy.columns.tolist()
            values = df_copy.values.tolist()
            data_to_upload = [headers] + values
            
            # Update worksheet
            worksheet.update(range_name='A1', values=data_to_upload)
            
        except Exception as e:
            raise Exception(f"Failed to add worksheet '{worksheet_name}': {str(e)}")

