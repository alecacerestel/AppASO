"""
Error handling utilities.
Manages error logging and recovery.
"""

import traceback
from datetime import datetime

from src.services import DriveService
from src.utils.notifications import EmailService


class ErrorHandler:
    """
    Handles errors, logging, and notifications.
    """
    
    def __init__(self, drive_service: DriveService = None):
        """
        Initialize error handler.
        
        Args:
            drive_service: Optional Drive service for uploading logs
        """
        self.drive_service = drive_service
        self.email_service = EmailService()
    
    def handle_error(self, error: Exception, context: str = "") -> None:
        """
        Handle an error by logging and sending notifications.
        
        Args:
            error: The exception that occurred
            context: Additional context about where the error occurred
        """
        execution_date = datetime.now()
        
        # Create error log content
        log_content = self._create_log_content(error, context, execution_date)
        
        # Print to console
        print("\n" + "="*50)
        print("ERROR OCCURRED")
        print("="*50)
        print(log_content)
        print("="*50 + "\n")
        
        # Upload log to Google Drive if service is available
        if self.drive_service:
            try:
                log_filename = f"error_log_{execution_date.strftime('%Y%m%d_%H%M%S')}.txt"
                self.drive_service.upload_log_file(log_content, log_filename)
                print(f"Error log uploaded to Google Drive: {log_filename}")
            except Exception as upload_error:
                print(f"Failed to upload error log: {str(upload_error)}")
        
        # Send email notification
        try:
            self.email_service.send_error_alert(log_content, execution_date)
        except Exception as email_error:
            print(f"Failed to send email notification: {str(email_error)}")
    
    def _create_log_content(self, error: Exception, context: str, execution_date: datetime) -> str:
        """
        Create formatted log content.
        
        Args:
            error: The exception
            context: Additional context
            execution_date: Date of execution
            
        Returns:
            Formatted log content
        """
        log_lines = [
            "="*80,
            "ETL PIPELINE ERROR LOG",
            "="*80,
            f"Timestamp: {execution_date.strftime('%Y-%m-%d %H:%M:%S')}",
            f"Context: {context if context else 'General execution error'}",
            "",
            "Error Type:",
            f"{type(error).__name__}",
            "",
            "Error Message:",
            f"{str(error)}",
            "",
            "Stack Trace:",
            traceback.format_exc(),
            "="*80
        ]
        
        return "\n".join(log_lines)
