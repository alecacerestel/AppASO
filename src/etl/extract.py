"""
Data extraction module.
Reads Excel/CSV files from Google Drive RAW folder.
"""

import pandas as pd
import io
from typing import Dict, Tuple
from googleapiclient.http import MediaIoBaseDownload

from src.config import settings
from src.etl.column_mapping import ColumnMapper


class DataExtractor:
    """
    Handles data extraction from Google Drive RAW folder.
    Reads 6 Excel files: keywords, installs, and users (Apple + Google each).
    """
    
    def __init__(self, drive_service):
        """
        Initialize data extractor.
        
        Args:
            drive_service: Google Drive API service instance
        """
        self.drive_service = drive_service
        self.raw_folder_id = settings.RAW_DATA_FOLDER_ID
        self.file_patterns = ColumnMapper.FILE_PATTERNS
    
    def extract_all_data(self) -> Dict[str, Tuple[pd.DataFrame, pd.DataFrame]]:
        """
        Extract all data from RAW folder.
        
        Returns:
            Dictionary with keys: "keywords", "installs", "users"
            Each value is a tuple: (apple_dataframe, google_dataframe)
            
        Raises:
            Exception: If any file cannot be read
        """
        print("\n[EXTRACTION] Starting data extraction from Google Drive...")
        
        # List all files in RAW folder
        file_list = self._list_files_in_folder()
        
        # Extract each data type
        keywords_apple, keywords_google = self._extract_keywords(file_list)
        installs_apple, installs_google = self._extract_installs(file_list)
        users_apple, users_google = self._extract_users(file_list)
        
        print(f"[EXTRACTION] Keywords - Apple: {len(keywords_apple)} rows, Google: {len(keywords_google)} rows")
        print(f"[EXTRACTION] Installs - Apple: {len(installs_apple)} rows, Google: {len(installs_google)} rows")
        print(f"[EXTRACTION] Users - Apple: {len(users_apple)} rows, Google: {len(users_google)} rows")
        
        return {
            "keywords": (keywords_apple, keywords_google),
            "installs": (installs_apple, installs_google),
            "users": (users_apple, users_google)
        }
    
    def _list_files_in_folder(self) -> Dict[str, str]:
        """
        List all files in the RAW folder.
        
        Returns:
            Dictionary mapping filename to file ID
        """
        query = f"'{self.raw_folder_id}' in parents and trashed=false"
        
        try:
            results = self.drive_service.files().list(
                q=query,
                spaces="drive",
                fields="files(id, name)",
                pageSize=100
            ).execute()
            
            files = results.get("files", [])
            file_dict = {file["name"]: file["id"] for file in files}
            
            print(f"[EXTRACTION] Found {len(file_dict)} files in RAW folder")
            
            return file_dict
            
        except Exception as e:
            raise Exception(f"Error listing files in RAW folder: {str(e)}")
    
    def _download_file_as_dataframe(self, file_id: str, filename: str) -> pd.DataFrame:
        """
        Download a file from Drive and convert to DataFrame.
        
        Args:
            file_id: Google Drive file ID
            filename: Name of the file (to determine format)
            
        Returns:
            DataFrame with file contents
        """
        try:
            request = self.drive_service.files().get_media(fileId=file_id)
            file_buffer = io.BytesIO()
            downloader = MediaIoBaseDownload(file_buffer, request)
            
            done = False
            while not done:
                status, done = downloader.next_chunk()
            
            file_buffer.seek(0)
            
            # Read based on file extension
            if filename.endswith('.xlsx') or filename.endswith('.xls'):
                df = pd.read_excel(file_buffer)
            elif filename.endswith('.csv'):
                df = pd.read_csv(file_buffer)
            else:
                raise Exception(f"Unsupported file format: {filename}")
            
            return df
            
        except Exception as e:
            raise Exception(f"Error downloading file '{filename}': {str(e)}")
    
    def _find_file_by_pattern(self, file_list: Dict[str, str], pattern: str) -> Tuple[str, str]:
        """
        Find a file in the list that matches the pattern.
        
        Args:
            file_list: Dictionary of filename -> file_id
            pattern: Pattern to search for in filename
            
        Returns:
            Tuple of (filename, file_id)
            
        Raises:
            Exception: If file not found
        """
        for filename, file_id in file_list.items():
            if pattern in filename:
                return filename, file_id
        
        raise Exception(f"File matching pattern '{pattern}' not found in RAW folder")
    
    def _extract_keywords(self, file_list: Dict[str, str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Extract keywords data from both platforms.
        
        Args:
            file_list: Dictionary of available files
            
        Returns:
            Tuple of (apple_df, google_df)
        """
        # Apple keywords
        apple_filename, apple_id = self._find_file_by_pattern(
            file_list, self.file_patterns["keywords_apple"]
        )
        apple_df = self._download_file_as_dataframe(apple_id, apple_filename)
        
        # Google keywords
        google_filename, google_id = self._find_file_by_pattern(
            file_list, self.file_patterns["keywords_google"]
        )
        google_df = self._download_file_as_dataframe(google_id, google_filename)
        
        return apple_df, google_df
    
    def _extract_installs(self, file_list: Dict[str, str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Extract installs data from both platforms.
        
        Args:
            file_list: Dictionary of available files
            
        Returns:
            Tuple of (apple_df, google_df)
        """
        # Apple installs
        apple_filename, apple_id = self._find_file_by_pattern(
            file_list, self.file_patterns["installs_apple"]
        )
        apple_df = self._download_file_as_dataframe(apple_id, apple_filename)
        
        # Google installs
        google_filename, google_id = self._find_file_by_pattern(
            file_list, self.file_patterns["installs_google"]
        )
        google_df = self._download_file_as_dataframe(google_id, google_filename)
        
        return apple_df, google_df
    
    def _extract_users(self, file_list: Dict[str, str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Extract active users data from both platforms.
        
        Args:
            file_list: Dictionary of available files
            
        Returns:
            Tuple of (apple_df, google_df)
        """
        # Apple users
        apple_filename, apple_id = self._find_file_by_pattern(
            file_list, self.file_patterns["users_apple"]
        )
        apple_df = self._download_file_as_dataframe(apple_id, apple_filename)
        
        # Google users
        google_filename, google_id = self._find_file_by_pattern(
            file_list, self.file_patterns["users_google"]
        )
        google_df = self._download_file_as_dataframe(google_id, google_filename)
        
        return apple_df, google_df
