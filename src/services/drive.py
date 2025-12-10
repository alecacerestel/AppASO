"""
Google Drive service layer.
Handles all interactions with Google Drive and Google Sheets.
"""

import gspread
from typing import Optional, Dict

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
            
            # Support checkboxes (TRUE/FALSE) and text (ON/OFF)
            if control_value is True or control_value == "TRUE":
                return True
            if isinstance(control_value, str) and control_value.strip().upper() == "ON":
                return True
            
            return False
            
        except Exception as e:
            raise Exception(f"Error checking control panel: {str(e)}")
    
    def get_control_panel_flags(self) -> Dict[str, bool]:
        """
        Read all control flags from Panel de Control.
        
        Returns:
            Dictionary with control flags:
            - execute_pipeline (B3): Main ON/OFF switch
            - run_backup (B4): Enable/disable local and Drive backup
            - run_ml (B5): Force ML retrain
            - send_alerts (B6): Enable/disable email notifications
            
        Raises:
            Exception: If control panel cannot be accessed
        """
        try:
            control_file_id = self.find_file_by_name(settings.CONTROL_PANEL_NAME)
            
            if not control_file_id:
                raise Exception(f"Control panel '{settings.CONTROL_PANEL_NAME}' not found")
            
            spreadsheet = self.gspread_client.open_by_key(control_file_id)
            worksheet = spreadsheet.worksheet(settings.CONTROL_SHEET_NAME)
            
            # Read cells B3, B4, B5, B6
            values = worksheet.batch_get(['B3', 'B4', 'B5', 'B6'])
            
            def parse_checkbox(cell_value):
                """Parse checkbox value to boolean"""
                if cell_value is None or len(cell_value) == 0 or len(cell_value[0]) == 0:
                    return False
                val = cell_value[0][0]
                if val is True or val == "TRUE":
                    return True
                if isinstance(val, str) and val.strip().upper() == "ON":
                    return True
                return False
            
            return {
                'execute_pipeline': parse_checkbox(values[0]),  # B3
                'run_backup': parse_checkbox(values[1]),        # B4
                'run_ml': parse_checkbox(values[2]),            # B5
                'send_alerts': parse_checkbox(values[3])        # B6
            }
            
        except Exception as e:
            raise Exception(f"Error reading control panel flags: {str(e)}")
