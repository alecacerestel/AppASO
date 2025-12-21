"""
Main ETL Pipeline execution script.
Orchestrates the complete ASO data pipeline with error handling.
"""

import sys
import os
from datetime import datetime

from src.services import AuthService, DriveService
from src.etl import ETLPipeline
from src.utils import ErrorHandler, EmailService
from src.config import settings


def main():
    """
    Main execution function for the ETL pipeline.
    """
    # Allow overriding execution date for testing (e.g., simulate Day 11)
    execution_date_str = os.getenv('EXECUTION_DATE')
    if execution_date_str:
        try:
            execution_date = datetime.strptime(execution_date_str, '%Y-%m-%d')
            print(f"[TEST MODE] Using overridden execution date: {execution_date_str}")
        except ValueError:
            print(f"[WARNING] Invalid EXECUTION_DATE format: {execution_date_str}. Using current date.")
            execution_date = datetime.now()
    else:
        execution_date = datetime.now()
    
    error_handler = ErrorHandler()
    
    try:
        print("="*70)
        print("AppASO ETL Pipeline - ASO Data Processing")
        print("="*70)
        print(f"Execution started: {execution_date.strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # Step 1: Authenticate with Google Cloud
        print("[AUTHENTICATION] Authenticating with Google Cloud...")
        auth_service = AuthService()
        gspread_client, drive_api = auth_service.authenticate()
        print("[AUTHENTICATION] Successfully authenticated")
        print()
        
        # Step 2: Initialize Drive service
        drive_service = DriveService(gspread_client, drive_api)
        
        # Step 3: Read control panel flags (B3, B4, B5, B6)
        print("[CONTROL CHECK] Reading control panel flags...")
        panel_flags = drive_service.get_control_panel_flags()
        
        # Check if pipeline execution is enabled (B3)
        if not panel_flags['execute_pipeline']:
            print("[CONTROL CHECK] Pipeline is DISABLED (Panel B3 = OFF)")
            print("Execution stopped gracefully. No data processing will occur.")
            print("To enable the pipeline, set cell B3 in 00_Control_Panel to TRUE")
            sys.exit(0)
        
        print("[CONTROL CHECK] Pipeline is ENABLED (Panel B3 = ON)")
        
        # Override settings with Panel values (unless env vars are set)
        if not os.getenv('RUN_BACKUP'):
            settings.RUN_BACKUP = panel_flags['run_backup']
            settings.RUN_BACKUP_DRIVE = panel_flags['run_backup']
        
        if not os.getenv('RUN_ML'):
            settings.RUN_ML = panel_flags['run_ml']
        
        if not os.getenv('SEND_ALERTS'):
            settings.SEND_ALERTS = panel_flags['send_alerts']
        
        # Display active flags
        print(f"  B4 (Backup):       {'ON' if settings.RUN_BACKUP else 'OFF'}")
        print(f"  B5 (Force ML):     {'ON' if settings.RUN_ML else 'OFF'}")
        print(f"  B6 (Send Alerts):  {'ON' if settings.SEND_ALERTS else 'OFF'}")
        print()
        
        # Step 4: Check ML flag (for future implementation)
        if settings.RUN_ML:
            print("[ML] Force ML retrain requested (Panel B5 = TRUE)")
            print("[ML] Feature not implemented yet - will be added in future version")
            print()
        
        # Step 5: Run ETL pipeline
        pipeline = ETLPipeline(drive_service)
        stats = pipeline.run(execution_date)
        
        # Step 6: Send success notification
        email_service = EmailService()
        email_service.send_success_notification(stats, execution_date)
        
        print()
        print("="*70)
        print("Pipeline execution completed successfully")
        print("="*70)
        
    except Exception as e:
        print()
        print("="*70)
        print("CRITICAL ERROR: Pipeline execution failed")
        print("="*70)
        
        # Handle error and send notifications
        error_handler.handle_error(e, "ETL Pipeline Execution")
        
        sys.exit(1)


if __name__ == "__main__":
    main()

