"""
Main ETL Pipeline execution script.
Orchestrates the complete data pipeline with error handling.
"""

import sys
from datetime import datetime

from src.services import AuthService, DriveService
from src.etl import ETLPipeline
from src.utils import ErrorHandler


def main():
    """
    Main execution function for the ETL pipeline.
    """
    execution_date = datetime.now()
    drive_service = None
    error_handler = None
    
    try:
        print("="*60)
        print("AppASO ETL Pipeline - Stage Environment")
        print("="*60)
        print(f"Execution started: {execution_date.strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # Step 1: Authenticate
        print("[Authentication] Authenticating with Google Cloud...")
        auth_service = AuthService()
        gspread_client, drive_api = auth_service.authenticate()
        print("[Authentication] Successfully authenticated")
        print()
        
        # Step 2: Initialize services
        drive_service = DriveService(gspread_client, drive_api)
        error_handler = ErrorHandler(drive_service)
        
        # Step 3: Check control panel
        print("[Control Check] Checking control panel status...")
        is_enabled = drive_service.check_control_panel()
        
        if not is_enabled:
            print("[Control Check] Pipeline is DISABLED (control panel is OFF)")
            print("Execution stopped. No data processing will occur.")
            sys.exit(0)
        
        print("[Control Check] Pipeline is ENABLED (control panel is ON)")
        print()
        
        # Step 4: Run ETL pipeline
        pipeline = ETLPipeline(drive_service)
        pipeline.run()
        
        print()
        print("="*60)
        print("Pipeline execution completed successfully")
        print("="*60)
        
    except Exception as e:
        print()
        print("="*60)
        print("CRITICAL ERROR: Pipeline execution failed")
        print("="*60)
        
        # Use error handler if available, otherwise print error
        if error_handler:
            error_handler.handle_error(e, "ETL Pipeline Execution")
        else:
            print(f"Error: {str(e)}")
            import traceback
            traceback.print_exc()
        
        sys.exit(1)


if __name__ == "__main__":
    main()
