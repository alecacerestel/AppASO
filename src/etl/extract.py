"""
Data extraction module.
Downloads Excel files from Google Drive RAW folder and saves them locally.
"""

import pandas as pd
import io
import os
from typing import Dict, Tuple
from googleapiclient.http import MediaIoBaseDownload

from src.config import settings
from src.etl.column_mapping import ColumnMapper


class DataExtractor:
    """
    Handles data extraction from Google Drive RAW folder.
    Downloads 6 Excel files and saves them to data/raw/ directory.
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
        self.local_data_dir = "data/raw"
        
        # Create local directory if it doesn't exist
        os.makedirs(self.local_data_dir, exist_ok=True)
    
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
        
        print(f"[EXTRACTION] All files downloaded to {self.local_data_dir}/")
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
    
    def _download_and_save_file(self, file_id: str, filename: str) -> str:
        """
        Download a file from Drive and save it locally.
        
        Args:
            file_id: Google Drive file ID
            filename: Name of the file to save
            
        Returns:
            Path to the saved file
            
        Raises:
            Exception: If download fails
        """
        local_path = os.path.join(self.local_data_dir, filename)
        
        try:
            print(f"[EXTRACTION] Downloading {filename}...")
            
            request = self.drive_service.files().get_media(fileId=file_id)
            file_buffer = io.BytesIO()
            downloader = MediaIoBaseDownload(file_buffer, request)
            
            done = False
            while not done:
                status, done = downloader.next_chunk()
            
            # Save to local file
            with open(local_path, 'wb') as f:
                f.write(file_buffer.getvalue())
            
            return local_path
            
        except Exception as e:
            raise Exception(f"Error downloading file '{filename}': {str(e)}")
    
    def _read_local_file(self, filepath: str) -> pd.DataFrame:
        """
        Read a local Excel or CSV file into a DataFrame.
        
        Args:
            filepath: Path to the local file
            
        Returns:
            DataFrame with file contents
            
        Raises:
            Exception: If file cannot be read
        """
        try:
            if filepath.endswith('.xlsx') or filepath.endswith('.xls'):
                df = pd.read_excel(filepath)
            elif filepath.endswith('.csv'):
                df = pd.read_csv(filepath)
            else:
                raise Exception(f"Unsupported file format: {filepath}")
            
            return df
            
        except Exception as e:
            raise Exception(f"Error reading file '{os.path.basename(filepath)}': {str(e)}")
    
    def _extract_keywords(self, file_list: Dict[str, str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Extract keywords data from both platforms.
        Downloads files and reads them from local storage.
        
        Args:
            file_list: Dictionary of available files
            
        Returns:
            Tuple of (apple_df, google_df)
        """
        # Apple keywords
        apple_filename, apple_id = self._find_file_by_pattern(
            file_list, self.file_patterns["keywords_apple"]
        )
        apple_path = self._download_and_save_file(apple_id, apple_filename)
        apple_df = self._read_local_file(apple_path)
        
        # Google keywords
        google_filename, google_id = self._find_file_by_pattern(
            file_list, self.file_patterns["keywords_google"]
        )
        google_path = self._download_and_save_file(google_id, google_filename)
        google_df = self._read_local_file(google_path)
        
        return apple_df, google_df
    
    def _extract_installs(self, file_list: Dict[str, str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Extract installs data from both platforms.
        Downloads files and reads them from local storage.
        
        Args:
            file_list: Dictionary of available files
            
        Returns:
            Tuple of (apple_df, google_df)
        """
        # Apple installs
        apple_filename, apple_id = self._find_file_by_pattern(
            file_list, self.file_patterns["installs_apple"]
        )
        apple_path = self._download_and_save_file(apple_id, apple_filename)
        apple_df = self._read_local_file(apple_path)
        
        # Google installs
        google_filename, google_id = self._find_file_by_pattern(
            file_list, self.file_patterns["installs_google"]
        )
        google_path = self._download_and_save_file(google_id, google_filename)
        google_df = self._read_local_file(google_path)
        
        return apple_df, google_df
    
    def _extract_users(self, file_list: Dict[str, str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Extract active users data from both platforms.
        Downloads files and reads them from local storage.
        
        Args:
            file_list: Dictionary of available files
            
        Returns:
            Tuple of (apple_df, google_df)
        """
        # Apple users
        apple_filename, apple_id = self._find_file_by_pattern(
            file_list, self.file_patterns["users_apple"]
        )
        apple_path = self._download_and_save_file(apple_id, apple_filename)
        apple_df = self._read_local_file(apple_path)
        
        # Google users
        google_filename, google_id = self._find_file_by_pattern(
            file_list, self.file_patterns["users_google"]
        )
        google_path = self._download_and_save_file(google_id, google_filename)
        google_df = self._read_local_file(google_path)
        
        return apple_df, google_df
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
