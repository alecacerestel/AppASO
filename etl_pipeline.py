"""
Main ETL Pipeline execution script.
Orchestrates the complete ASO data pipeline with error handling.
"""

import sys
from datetime import datetime

from src.services import AuthService, DriveService
from src.etl import ETLPipeline
from src.utils import ErrorHandler, EmailService


def main():
    """
    Main execution function for the ETL pipeline.
    """
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
        
        # Step 3: Check control panel (cell B3 must be TRUE/ON)
        print("[CONTROL CHECK] Verifying control panel status...")
        is_enabled = drive_service.check_control_panel()
        
        if not is_enabled:
            print("[CONTROL CHECK] Pipeline is DISABLED (control panel B3 is OFF)")
            print("Execution stopped gracefully. No data processing will occur.")
            print("To enable the pipeline, set cell B3 in 00_Control_Panel to TRUE or ON")
            sys.exit(0)
        
        print("[CONTROL CHECK] Pipeline is ENABLED (control panel B3 is ON)")
        print()
        
        # Step 4: Run ETL pipeline
        pipeline = ETLPipeline(drive_service)
        stats = pipeline.run()
        
        # Step 5: Send success notification
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

