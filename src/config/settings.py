"""
Configuration settings for the ETL pipeline.
Manages environment variables and application constants.
"""

import os
import json
from typing import Dict, Any


class Settings:
    """
    Application settings and environment variable management.
    """
    
    # Google Cloud credentials
    GCP_JSON: str = os.getenv("GCP_JSON", "")
    
    # Email configuration
    SMTP_HOST: str = os.getenv("SMTP_HOST", "smtp.gmail.com")
    SMTP_PORT: int = int(os.getenv("SMTP_PORT", "587"))
    EMAIL_USER: str = os.getenv("EMAIL_USER", "")
    EMAIL_PASSWORD: str = os.getenv("EMAIL_PASSWORD", "")
    EMAIL_RECIPIENT: str = os.getenv("EMAIL_RECIPIENT", "")
    
    # Google Drive structure constants
    CONTROL_PANEL_NAME: str = "00_Control_Panel"
    CONTROL_SHEET_NAME: str = "Config"
    CONTROL_CELL: str = "B3"
    
    # Raw data folder (contains source Excel/CSV files)
    RAW_DATA_FOLDER_ID: str = "1HptFA1vpGiLZaLzZZZO5wI0P3EjKTDlL"
    
    # Master data sheet (destination with 3 worksheets)
    MASTER_DATA_SHEET: str = "MASTER_DATA_CLEAN"
    
    # Worksheet names in MASTER_DATA_CLEAN
    KEYWORDS_SHEET: str = "KEYWORDS"
    INSTALLS_SHEET: str = "INSTALLS"
    USERS_SHEET: str = "USERS"
    
    # Historical data lake for daily backups
    DATA_LAKE_FOLDER: str = "02_Data_Lake_Historic"
    
    # Agency start date for business logic (Pre-Agency vs Con-Agency)
    AGENCY_START_DATE: str = "2025-07-15"
    
    # Month names mapping
    MONTH_NAMES: Dict[int, str] = {
        1: "01_January",
        2: "02_February",
        3: "03_March",
        4: "04_April",
        5: "05_May",
        6: "06_June",
        7: "07_July",
        8: "08_August",
        9: "09_September",
        10: "10_October",
        11: "11_November",
        12: "12_December"
    }
    
    @classmethod
    def get_credentials_dict(cls) -> Dict[str, Any]:
        """
        Parse GCP_JSON environment variable into a dictionary.
        
        Returns:
            Dictionary containing Google Cloud service account credentials
            
        Raises:
            ValueError: If GCP_JSON is not set or invalid
        """
        if not cls.GCP_JSON:
            raise ValueError("GCP_JSON environment variable is not set")
        
        try:
            return json.loads(cls.GCP_JSON)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in GCP_JSON environment variable: {e}")
    
    @classmethod
    def validate_email_config(cls) -> bool:
        """
        Validate that all required email configuration is present.
        
        Returns:
            True if all email settings are configured
        """
        return all([
            cls.EMAIL_USER,
            cls.EMAIL_PASSWORD,
            cls.EMAIL_RECIPIENT
        ])


settings = Settings()
