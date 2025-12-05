"""
Authentication service for Google Cloud Platform.
Handles credential management and service initialization.
"""

from google.oauth2.service_account import Credentials
import gspread
from googleapiclient.discovery import build
from typing import Tuple

from src.config import settings


class AuthService:
    """
    Manages authentication with Google Cloud services.
    """
    
    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    
    def __init__(self):
        """
        Initialize authentication service with credentials from environment.
        """
        self.credentials = None
        self.gspread_client = None
        self.drive_service = None
    
    def authenticate(self) -> Tuple[gspread.Client, any]:
        """
        Authenticate with Google Cloud and initialize service clients.
        
        Returns:
            Tuple containing (gspread_client, drive_service)
            
        Raises:
            ValueError: If credentials are invalid or missing
        """
        try:
            creds_dict = settings.get_credentials_dict()
            
            self.credentials = Credentials.from_service_account_info(
                creds_dict,
                scopes=self.SCOPES
            )
            
            # Initialize gspread client for Google Sheets
            self.gspread_client = gspread.authorize(self.credentials)
            
            # Initialize Google Drive API service
            self.drive_service = build("drive", "v3", credentials=self.credentials)
            
            return self.gspread_client, self.drive_service
            
        except Exception as e:
            raise ValueError(f"Authentication failed: {str(e)}")
    
    def get_clients(self) -> Tuple[gspread.Client, any]:
        """
        Get authenticated clients. Authenticates if not already done.
        
        Returns:
            Tuple containing (gspread_client, drive_service)
        """
        if self.gspread_client is None or self.drive_service is None:
            return self.authenticate()
        
        return self.gspread_client, self.drive_service
