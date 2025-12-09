"""
Data transformation module.
Handles data cleaning, standardization, and business logic.
"""

import pandas as pd
from datetime import datetime
from typing import Dict, Tuple

from src.config import settings
from src.etl.column_mapping import ColumnMapper


class DataTransformer:
    """
    Handles data transformation, cleaning, and business logic operations.
    Applies column mapping, date formatting, and agency stage classification.
    """
    
    def __init__(self):
        """
        Initialize data transformer.
        """
        self.mapper = ColumnMapper()
        self.agency_start_date = pd.to_datetime(settings.AGENCY_START_DATE)
    
    def transform_all_data(self, raw_data: Dict[str, Tuple[pd.DataFrame, pd.DataFrame]]) -> Dict[str, pd.DataFrame]:
        """
        Transform all extracted data.
        
        Args:
            raw_data: Dictionary with "keywords", "installs", "users" as keys
                     Each value is a tuple of (apple_df, google_df)
        
        Returns:
            Dictionary with "keywords", "installs", "users" as keys
            Each value is a unified, cleaned DataFrame
        """
        print("\n[TRANSFORMATION] Starting data transformation...")
        
        # Transform each data type
        keywords_df = self._transform_keywords(
            raw_data["keywords"][0],  # Apple
            raw_data["keywords"][1]   # Google
        )
        
        installs_df = self._transform_installs(
            raw_data["installs"][0],  # Apple
            raw_data["installs"][1]   # Google
        )
        
        users_df = self._transform_users(
            raw_data["users"][0],  # Apple
            raw_data["users"][1]   # Google
        )
        
        print(f"[TRANSFORMATION] Keywords: {len(keywords_df)} total rows")
        print(f"[TRANSFORMATION] Installs: {len(installs_df)} total rows")
        print(f"[TRANSFORMATION] Users: {len(users_df)} total rows")
        
        return {
            "keywords": keywords_df,
            "installs": installs_df,
            "users": users_df
        }
    
    def _transform_keywords(self, apple_df: pd.DataFrame, google_df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform and unify keywords data from both platforms.
        
        Args:
            apple_df: Raw Apple keywords data
            google_df: Raw Google keywords data
        
        Returns:
            Unified and cleaned keywords DataFrame
        """
        # Apply column mapping for Apple
        apple_mapping = self.mapper.get_keywords_mapping("Apple")
        apple_clean = apple_df.rename(columns=apple_mapping)
        apple_clean["Platform"] = "Apple"
        
        # Apply column mapping for Google
        google_mapping = self.mapper.get_keywords_mapping("Google")
        google_clean = google_df.rename(columns=google_mapping)
        google_clean["Platform"] = "Google"
        
        # Combine both platforms
        combined = pd.concat([apple_clean, google_clean], ignore_index=True)
        
        # Keep only standard columns
        columns_to_keep = [col for col in self.mapper.get_columns_to_keep("keywords") if col != "Stage"]
        combined = combined[columns_to_keep]
        
        # Convert to datetime for proper chronological sorting
        combined["Date"] = pd.to_datetime(combined["Date"], errors="coerce")
        combined = combined.sort_values(["Date", "Platform"]).reset_index(drop=True)
        
        # Format dates to DD/MM/YYYY after sorting
        combined["Date"] = combined["Date"].dt.strftime("%d/%m/%Y")
        
        # Add business logic and handle nulls
        combined = self._add_agency_stage(combined)
        combined = self._handle_nulls(combined)
        
        return combined
    
    def _transform_installs(self, apple_df: pd.DataFrame, google_df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform and unify installs data from both platforms.
        
        Args:
            apple_df: Raw Apple installs data
            google_df: Raw Google installs data
        
        Returns:
            Unified and cleaned installs DataFrame
        """
        # Apply column mapping for Apple
        apple_mapping = self.mapper.get_installs_mapping("Apple")
        apple_clean = apple_df.rename(columns=apple_mapping)
        apple_clean["Platform"] = "Apple"
        
        # Apply column mapping for Google
        google_mapping = self.mapper.get_installs_mapping("Google")
        google_clean = google_df.rename(columns=google_mapping)
        google_clean["Platform"] = "Google"
        
        # Combine both platforms
        combined = pd.concat([apple_clean, google_clean], ignore_index=True)
        
        # Keep only standard columns
        columns_to_keep = [col for col in self.mapper.get_columns_to_keep("installs") if col != "Stage"]
        combined = combined[columns_to_keep]
        
        # Clean Installs: remove ALL types of spaces from numbers (Google uses spaces like "1 042")
        combined["Installs"] = combined["Installs"].astype(str).str.replace(r'\s+', '', regex=True)
        
        # Ensure Installs is numeric
        combined["Installs"] = pd.to_numeric(combined["Installs"], errors="coerce")
        
        # Convert to datetime for proper chronological sorting
        combined["Date"] = pd.to_datetime(combined["Date"], errors="coerce")
        combined = combined.sort_values(["Date", "Platform"]).reset_index(drop=True)
        
        # Format dates to DD/MM/YYYY after sorting
        combined["Date"] = combined["Date"].dt.strftime("%d/%m/%Y")
        
        # Add business logic and handle nulls
        combined = self._add_agency_stage(combined)
        combined = self._handle_nulls(combined)
        
        return combined
    
    def _transform_users(self, apple_df: pd.DataFrame, google_df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform and unify active users data from both platforms.
        Special handling required due to different file structures.
        
        Args:
            apple_df: Raw Apple users data
            google_df: Raw Google users data
        
        Returns:
            Unified and cleaned users DataFrame
        """
        # Apple data has metadata rows at the top - need to skip them
        apple_clean = self._clean_apple_users(apple_df)
        
        # Google data has different date format
        google_clean = self._clean_google_users(google_df)
        
        # Combine both platforms
        combined = pd.concat([apple_clean, google_clean], ignore_index=True)
        
        # Keep only standard columns
        columns_to_keep = [col for col in self.mapper.get_columns_to_keep("users") if col != "Stage"]
        combined = combined[columns_to_keep]
        
        # Remove any rows with invalid dates or users (already numeric from _clean_google_users)
        combined = combined.dropna(subset=["Date", "Active_Users"])
        
        # Convert to datetime for proper chronological sorting
        combined["Date"] = pd.to_datetime(combined["Date"], errors="coerce")
        combined = combined.sort_values(["Date", "Platform"]).reset_index(drop=True)
        
        # Format dates to DD/MM/YYYY after sorting
        combined["Date"] = combined["Date"].dt.strftime("%d/%m/%Y")
        
        # Add business logic and handle nulls
        combined = self._add_agency_stage(combined)
        combined = self._handle_nulls(combined)
        
        return combined
    
    def _clean_apple_users(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Apple users data (has metadata rows at top).
        
        Args:
            df: Raw Apple users DataFrame
        
        Returns:
            Cleaned DataFrame
        """
        # Apply column mapping
        mapping = self.mapper.get_users_mapping("Apple")
        df_mapped = df.rename(columns=mapping)
        
        # Skip first 2 rows (metadata: start date, end date)
        # Then skip any NaN rows
        df_clean = df_mapped.iloc[2:].copy()
        df_clean = df_clean.dropna(subset=["Date", "Active_Users"])
        
        # Add platform identifier and empty Notes column
        df_clean["Platform"] = "Apple"
        df_clean["Notes"] = ""
        
        return df_clean
    
    def _clean_google_users(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Google users data (has French date format).
        
        Args:
            df: Raw Google users DataFrame
        
        Returns:
            Cleaned DataFrame
        """
        # Apply column mapping
        mapping = self.mapper.get_users_mapping("Google")
        df_mapped = df.rename(columns=mapping)
        
        # Select needed columns (including Notes)
        df_clean = df_mapped[["Date", "Active_Users", "Notes"]].copy()
        
        # Clean Active_Users: remove ALL types of spaces from numbers
        # Google uses \u202f (narrow no-break space), \xa0 (non-breaking space), and regular spaces
        df_clean["Active_Users"] = df_clean["Active_Users"].astype(str).str.replace(r'\s+', '', regex=True)
        
        # Convert to numeric
        df_clean["Active_Users"] = pd.to_numeric(df_clean["Active_Users"], errors="coerce")
        
        # Remove any rows with missing Date or Active_Users
        df_clean = df_clean.dropna(subset=["Date", "Active_Users"])
        
        # Convert French date format (e.g., "1 janv. 2024") to standard format
        df_clean["Date"] = self._parse_french_dates(df_clean["Date"])
        
        # Fill NaN Notes with empty string
        df_clean["Notes"] = df_clean["Notes"].fillna("")
        
        # Add platform identifier
        df_clean["Platform"] = "Google"
        
        return df_clean
    
    def _parse_french_dates(self, date_series: pd.Series) -> pd.Series:
        """
        Parse French formatted dates to standard datetime.
        Handles format like "1 janv. 2024" or "15 déc. 2024"
        
        Args:
            date_series: Series with French formatted dates
        
        Returns:
            Series with parsed datetime values
        """
        # French month abbreviations mapping
        french_months = {
            "janv": "01", "févr": "02", "mars": "03", "avr": "04",
            "mai": "05", "juin": "06", "juil": "07", "août": "08",
            "sept": "09", "oct": "10", "nov": "11", "déc": "12"
        }
        
        def parse_single_date(date_str):
            try:
                # Remove periods and split
                parts = str(date_str).replace(".", "").split()
                if len(parts) == 3:
                    day = parts[0].zfill(2)
                    month = french_months.get(parts[1], "01")
                    year = parts[2]
                    return f"{year}-{month}-{day}"
                return None
            except:
                return None
        
        parsed = date_series.apply(parse_single_date)
        return pd.to_datetime(parsed, errors="coerce")
    
    def _format_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Ensure Date column is in DD/MM/YYYY format (standard European format).
        
        Args:
            df: DataFrame with Date column
        
        Returns:
            DataFrame with formatted dates as DD/MM/YYYY strings
        """
        if "Date" in df.columns:
            # Convert to datetime first
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            # Format as DD/MM/YYYY
            df["Date"] = df["Date"].dt.strftime("%d/%m/%Y")
        
        return df
    
    def _add_agency_stage(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add business logic column: Pré-Agence vs Avec-Agence.
        Based on agency start date (2025-07-15).
        
        Args:
            df: DataFrame with Date column (DD/MM/YYYY format)
        
        Returns:
            DataFrame with Stage column added
        """
        if "Date" in df.columns:
            # Convert DD/MM/YYYY string to datetime for comparison
            date_temp = pd.to_datetime(df["Date"], format="%d/%m/%Y", errors="coerce")
            df["Stage"] = date_temp.apply(
                lambda x: "Avec-Agence" if x >= self.agency_start_date else "Pré-Agence"
            )
        
        return df
    
    def _handle_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle null values in the DataFrame.
        
        Args:
            df: DataFrame with potential null values
        
        Returns:
            DataFrame with nulls handled
        """
        # Drop rows with null dates (critical column)
        if "Date" in df.columns:
            df = df.dropna(subset=["Date"])
        
        # For numeric columns, keep nulls as they might be legitimate missing data
        # They will be handled in visualization layer
        
        return df
