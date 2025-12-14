"""
This script trains Linear Regression models to forecast app installs
for the upcoming month, separately for Google and Apple platforms.
Writes results to MASTER_DATA_CLEAN Google Sheet as a new "FORECAST" 
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sklearn.linear_model import LinearRegression
from pathlib import Path

# Add parent directory to path to import src modules
sys.path.append(str(Path(__file__).parent.parent))

from src.services import AuthService, DriveService
from src.config import settings


class InstallsForecaster:
    """
    Forecasting pipeline for app installs prediction.
    Uses Linear Regression models trained on recent historical data.
    Writes results to Google Sheets.
    """
    
    def __init__(self, processed_folder: str, output_folder: str, drive_service: DriveService = None, training_months: int = 4):
        """
        Initialize the forecaster.
        
        Args:
            processed_folder: Path to folder containing processed data
            output_folder: Path to ML folder for saving forecasts (local backup)
            drive_service: Authenticated Drive service for Google Sheets access
            training_months: Number of recent months to use for training (default: 4)
        """
        self.processed_folder = Path(processed_folder)
        self.output_folder = Path(output_folder)
        self.drive_service = drive_service
        self.training_months = training_months
        self.models = {}
        
    def load_data(self) -> pd.DataFrame:
        """
        Load and combine all install CSV files from processed folder.
        
        Returns:
            DataFrame with Date, Installs, Platform, Stage columns
        """
        print("[1/5] Loading install data...")
        
        # Find all installs CSV files
        install_files = list(self.processed_folder.glob("installs_*.csv"))
        
        if not install_files:
            raise FileNotFoundError(f"No install files found in {self.processed_folder}")
        
        # Load and combine all files
        dfs = []
        for file in install_files:
            df = pd.read_csv(file)
            dfs.append(df)
        
        data = pd.concat(dfs, ignore_index=True)
        print(f"   Loaded {len(data)} records from {len(install_files)} files")
        
        return data
    
    def preprocess_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Parse dates and filter to recent months for training.
        
        Args:
            data: Raw data with Date column in DD/MM/YYYY format
            
        Returns:
            Preprocessed DataFrame with datetime objects and filtered data
        """
        print("[2/5] Preprocessing data...")
        
        # Parse dates from DD/MM/YYYY format
        data['Date'] = pd.to_datetime(data['Date'], format='%d/%m/%Y')
        
        # Filter to last N months
        cutoff_date = datetime.now() - timedelta(days=self.training_months * 30)
        data_filtered = data[data['Date'] >= cutoff_date].copy()
        
        # Convert date to ordinal for model training
        data_filtered['date_ordinal'] = data_filtered['Date'].apply(lambda x: x.toordinal())
        
        # Sort by date
        data_filtered = data_filtered.sort_values('Date').reset_index(drop=True)
        
        print(f"   Using data from {data_filtered['Date'].min().strftime('%Y-%m-%d')} to {data_filtered['Date'].max().strftime('%Y-%m-%d')}")
        print(f"   Total records for training: {len(data_filtered)}")
        
        return data_filtered
    
    def train_models(self, data: pd.DataFrame):
        """
        Train separate Linear Regression models for Google and Apple platforms.
        
        Args:
            data: Preprocessed data with date_ordinal and Installs columns
        """
        print("[3/5] Training models...")
        
        platforms = data['Platform'].unique()
        
        for platform in platforms:
            # Filter data for this platform
            platform_data = data[data['Platform'] == platform]
            
            # Prepare features and target
            X = platform_data[['date_ordinal']].values
            y = platform_data['Installs'].values
            
            # Train Linear Regression model
            model = LinearRegression()
            model.fit(X, y)
            
            # Store model
            self.models[platform] = model
            
            # Calculate R-squared score
            score = model.score(X, y)
            print(f"   {platform}: R² = {score:.4f}")
    
    def generate_forecast(self) -> pd.DataFrame:
        """
        Generate daily install forecasts for the next month.
        
        Returns:
            DataFrame with Date, Installs, Platform columns
        """
        print("[4/5] Generating forecasts...")
        
        # Determine the next month's date range
        today = datetime.now()
        
        # Start from the first day of next month
        if today.month == 12:
            start_date = datetime(today.year + 1, 1, 1)
        else:
            start_date = datetime(today.year, today.month + 1, 1)
        
        # End at the last day of next month
        if start_date.month == 12:
            end_date = datetime(start_date.year + 1, 1, 1) - timedelta(days=1)
        else:
            end_date = datetime(start_date.year, start_date.month + 1, 1) - timedelta(days=1)
        
        # Generate date range
        forecast_dates = pd.date_range(start=start_date, end=end_date, freq='D')
        
        print(f"   Forecasting from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')} ({len(forecast_dates)} days)")
        
        # Generate predictions for each platform
        forecasts = []
        
        for platform, model in self.models.items():
            # Convert dates to ordinal
            date_ordinals = np.array([d.toordinal() for d in forecast_dates]).reshape(-1, 1)
            
            # Predict
            predictions = model.predict(date_ordinals)
            
            # Round to integers and ensure non-negative
            predictions = np.maximum(0, np.round(predictions)).astype(int)
            
            # Create DataFrame
            platform_forecast = pd.DataFrame({
                'Date': forecast_dates,
                'Installs': predictions,
                'Platform': platform
            })
            
            forecasts.append(platform_forecast)
            print(f"   {platform}: Average predicted installs = {predictions.mean():.0f}")
        
        # Combine all platforms
        forecast_df = pd.concat(forecasts, ignore_index=True)
        
        # Format date as DD/MM/YYYY for consistency
        forecast_df['Date'] = forecast_df['Date'].dt.strftime('%d/%m/%Y')
        
        return forecast_df
    
    def save_forecast(self, forecast_df: pd.DataFrame):
        """
        Save forecast to Google Sheets and local CSV backup.
        
        Args:
            forecast_df: DataFrame with forecast data
        """
        print("[5/5] Saving forecast...")
        
        # Save to Google Sheets as new worksheet in MASTER_DATA_CLEAN
        if self.drive_service:
            self._save_to_google_sheets(forecast_df)
        else:
            print("   Warning: Drive service not available. Skipping Google Sheets upload.")
        
        # Save local CSV backup
        self.output_folder.mkdir(parents=True, exist_ok=True)
        output_path = self.output_folder / "MASTER_DATA_FORECAST.csv"
        forecast_df.to_csv(output_path, index=False)
        
        print(f"   Local backup saved to: {output_path}")
        print(f"   Total forecasted records: {len(forecast_df)}")
    
    def _save_to_google_sheets(self, forecast_df: pd.DataFrame):
        """
        Write forecast to MASTER_DATA_CLEAN Google Sheet as "FORECAST" worksheet.
        
        Args:
            forecast_df: DataFrame with forecast data
        """
        try:
            print("   Writing to MASTER_DATA_CLEAN Google Sheet...")
            
            # Find the MASTER_DATA_CLEAN sheet
            master_file_id = self.drive_service.find_file_by_name(settings.MASTER_DATA_SHEET)
            
            if not master_file_id:
                raise Exception(f"Sheet '{settings.MASTER_DATA_SHEET}' not found")
            
            spreadsheet = self.drive_service.gspread_client.open_by_key(master_file_id)
            
            # Get or create FORECAST worksheet
            try:
                worksheet = spreadsheet.worksheet("FORECAST")
                print("   Found existing FORECAST worksheet. Updating...")
            except:
                worksheet = spreadsheet.add_worksheet(title="FORECAST", rows=1000, cols=10)
                print("   Created new FORECAST worksheet")
            
            # Clear existing content
            worksheet.clear()
            
            # Prepare data (dates are already in DD/MM/YYYY format)
            df_copy = forecast_df.copy()
            df_copy = df_copy.replace([float('inf'), float('-inf')], None)
            df_copy = df_copy.fillna('')
            
            # Prepare data with headers
            data = [df_copy.columns.tolist()] + df_copy.values.tolist()
            
            # Update worksheet
            worksheet.update(data, value_input_option="RAW")
            
            print(f"   Successfully updated FORECAST worksheet: {len(forecast_df)} rows")
            
        except Exception as e:
            print(f"   Error writing to Google Sheets: {str(e)}")
            print("   Forecast saved locally only.")
    
    def run_pipeline(self):
        """
        Execute the complete forecasting pipeline.
        """
        print("\n" + "="*60)
        print("INSTALLS FORECASTING PIPELINE")
        print("="*60 + "\n")
        
        try:
            # Step 1: Load data
            data = self.load_data()
            
            # Step 2: Preprocess data
            data_processed = self.preprocess_data(data)
            
            # Step 3: Train models
            self.train_models(data_processed)
            
            # Step 4: Generate forecasts
            forecast = self.generate_forecast()
            
            # Step 5: Save results
            self.save_forecast(forecast)
            
            print("\n" + "="*60)
            print("PIPELINE COMPLETED SUCCESSFULLY")
            print("="*60 + "\n")
            
        except Exception as e:
            print(f"\nERROR: Pipeline failed with exception:")
            print(f"   {type(e).__name__}: {str(e)}")
            raise


def main():
    """
    Main function to run the forecasting pipeline.
    """
    # Define paths relative to script location
    script_dir = Path(__file__).parent.parent
    processed_folder = script_dir / "data" / "processed"
    ml_folder = script_dir / "ML"
    
    # Initialize Drive service for Google Sheets access
    drive_service = None
    try:
        print("Initializing Google Drive service...")
        auth_service = AuthService()
        gspread_client, drive_api = auth_service.authenticate()
        drive_service = DriveService(gspread_client, drive_api)
        print("Google Drive service initialized successfully\n")
    except Exception as e:
        print(f"Warning: Could not initialize Drive service: {str(e)}")
        print("Will save forecast locally only\n")
    
    # Initialize and run forecaster
    forecaster = InstallsForecaster(
        processed_folder=str(processed_folder),
        output_folder=str(ml_folder),
        drive_service=drive_service,
        training_months=4
    )
    
    forecaster.run_pipeline()


if __name__ == "__main__":
    main()
