"""
Data transformation module.
Handles data cleaning and transformation logic.
"""

import pandas as pd
from datetime import datetime


class DataTransformer:
    """
    Handles data transformation and cleaning operations.
    """
    
    def __init__(self):
        """
        Initialize data transformer.
        """
        pass
    
    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform and clean the input DataFrame.
        
        Args:
            df: Raw DataFrame to transform
            
        Returns:
            Cleaned and transformed DataFrame
        """
        df_clean = df.copy()
        
        # Remove null values
        df_clean = self._handle_nulls(df_clean)
        
        # Format date column
        df_clean = self._format_dates(df_clean)
        
        # Validate data types
        df_clean = self._validate_types(df_clean)
        
        # Sort by id
        df_clean = df_clean.sort_values("id").reset_index(drop=True)
        
        return df_clean
    
    def _handle_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle null values in the DataFrame.
        
        Args:
            df: DataFrame with potential null values
            
        Returns:
            DataFrame with nulls handled
        """
        # For numeric columns, fill with median
        if "valor" in df.columns:
            df["valor"].fillna(df["valor"].median(), inplace=True)
        
        # Drop rows with nulls in critical columns
        critical_columns = ["id", "fecha", "fuente", "categoria"]
        df.dropna(subset=[col for col in critical_columns if col in df.columns], inplace=True)
        
        return df
    
    def _format_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Ensure date column is in YYYY-MM-DD format.
        
        Args:
            df: DataFrame with date column
            
        Returns:
            DataFrame with formatted dates
        """
        if "fecha" in df.columns:
            df["fecha"] = pd.to_datetime(df["fecha"]).dt.strftime("%Y-%m-%d")
        
        return df
    
    def _validate_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate and convert data types.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            DataFrame with validated types
        """
        # Ensure id is integer
        if "id" in df.columns:
            df["id"] = df["id"].astype(int)
        
        # Ensure valor is float
        if "valor" in df.columns:
            df["valor"] = df["valor"].astype(float)
        
        # Ensure categorical columns are strings
        for col in ["fuente", "categoria"]:
            if col in df.columns:
                df[col] = df[col].astype(str)
        
        return df
