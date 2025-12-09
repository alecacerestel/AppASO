"""
Google Drive service layer.
Handles all interactions with Google Drive and Google Sheets.
"""

import gspread
from googleapiclient.http import MediaFileUpload, MediaIoBaseUpload
from datetime import datetime
from typing import Optional, Dict, Any
import io
import pandas as pd

from src.config import settings


class DriveService:
    """
    Service class for Google Drive and Google Sheets operations.
    """
    
    def __init__(self, gspread_client: gspread.Client, drive_service: any):
        """
        Initialize Drive service with authenticated clients.
        
        Args:
            gspread_client: Authenticated gspread client
            drive_service: Authenticated Google Drive API service
        """
        self.gspread_client = gspread_client
        self.drive_service = drive_service
    
    def find_folder_by_name(self, folder_name: str, parent_id: Optional[str] = None) -> Optional[str]:
        """
        Find a folder by name in Google Drive.
        
        Args:
            folder_name: Name of the folder to find
            parent_id: Optional parent folder ID to search within
            
        Returns:
            Folder ID if found, None otherwise
        """
        query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
        if parent_id:
            query += f" and '{parent_id}' in parents"
        
        try:
            results = self.drive_service.files().list(
                q=query,
                spaces="drive",
                fields="files(id, name)",
                pageSize=1
            ).execute()
            
            files = results.get("files", [])
            return files[0]["id"] if files else None
            
        except Exception as e:
            raise Exception(f"Error finding folder '{folder_name}': {str(e)}")
    
    def find_file_by_name(self, file_name: str, parent_id: Optional[str] = None) -> Optional[str]:
        """
        Find a file by name in Google Drive.
        
        Args:
            file_name: Name of the file to find
            parent_id: Optional parent folder ID to search within
            
        Returns:
            File ID if found, None otherwise
        """
        query = f"name='{file_name}' and trashed=false"
        if parent_id:
            query += f" and '{parent_id}' in parents"
        
        try:
            results = self.drive_service.files().list(
                q=query,
                spaces="drive",
                fields="files(id, name)",
                pageSize=1
            ).execute()
            
            files = results.get("files", [])
            return files[0]["id"] if files else None
            
        except Exception as e:
            raise Exception(f"Error finding file '{file_name}': {str(e)}")
    
    def check_control_panel(self) -> bool:
        """
        Check if the control panel allows pipeline execution.
        
        Returns:
            True if control cell contains "ON", False otherwise
            
        Raises:
            Exception: If control panel cannot be accessed
        """
        try:
            control_file_id = self.find_file_by_name(settings.CONTROL_PANEL_NAME)
            
            if not control_file_id:
                raise Exception(f"Control panel '{settings.CONTROL_PANEL_NAME}' not found")
            
            spreadsheet = self.gspread_client.open_by_key(control_file_id)
            worksheet = spreadsheet.worksheet(settings.CONTROL_SHEET_NAME)
            control_value = worksheet.acell(settings.CONTROL_CELL).value
            
            return control_value and control_value.strip().upper() == "ON"
            
        except Exception as e:
            raise Exception(f"Error checking control panel: {str(e)}")

    
    def save_to_data_lake(self, df: pd.DataFrame, date: datetime, data_type: str) -> None:
        """
        Save DataFrame as CSV to the appropriate Data Lake folder.
        Creates a separate file for each data type (keywords, installs, users).
        
        Args:
            df: DataFrame to save
            date: Date to determine folder structure and filename
            data_type: Type of data ("keywords", "installs", or "users")
            
        Raises:
            Exception: If save fails
        """
        try:
            data_lake_id = self.find_folder_by_name(settings.DATA_LAKE_FOLDER)
            
            if not data_lake_id:
                raise Exception(f"Folder '{settings.DATA_LAKE_FOLDER}' not found")
            
            # Navigate to year folder
            year_folder = str(date.year)
            year_id = self.find_folder_by_name(year_folder, data_lake_id)
            
            if not year_id:
                raise Exception(f"Year folder '{year_folder}' not found in Data Lake")
            
            # Navigate to month folder
            month_folder = settings.MONTH_NAMES[date.month]
            month_id = self.find_folder_by_name(month_folder, year_id)
            
            if not month_id:
                raise Exception(f"Month folder '{month_folder}' not found in year '{year_folder}'")
            
            # Create CSV file in memory
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_content = csv_buffer.getvalue()
            
            # Upload file with data type in filename
            filename = f"{data_type}_{date.strftime('%Y%m%d')}.csv"
            
            # Check if file already exists
            existing_file_id = self.find_file_by_name(filename, month_id)
            
            file_metadata = {
                "name": filename,
                "parents": [month_id]
            }
            
            media = MediaIoBaseUpload(
                io.BytesIO(csv_content.encode("utf-8")),
                mimetype="text/csv",
                resumable=True
            )
            
            if existing_file_id:
                # Update existing file
                self.drive_service.files().update(
                    fileId=existing_file_id,
                    media_body=media
                ).execute()
            else:
                # Create new file
                self.drive_service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields="id"
                ).execute()
                
        except Exception as e:
            raise Exception(f"Error saving {data_type} to Data Lake: {str(e)}")

