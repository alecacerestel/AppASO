"""
Email notification service.
Handles sending error alerts via email.
"""

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

from src.config import settings


class EmailService:
    """
    Service for sending email notifications.
    """
    
    def __init__(self):
        """
        Initialize email service.
        """
        self.smtp_host = settings.SMTP_HOST
        self.smtp_port = settings.SMTP_PORT
        self.email_user = settings.EMAIL_USER
        self.email_password = settings.EMAIL_PASSWORD
        self.email_recipient = settings.EMAIL_RECIPIENT
    
    def send_success_notification(self, stats: dict, execution_date: datetime) -> None:
        """
        Send a success notification email with execution statistics.
        
        Args:
            stats: Dictionary with execution statistics (keywords, installs, users counts)
            execution_date: Date of the successful execution
            
        Raises:
            Exception: If email sending fails
        """
        if not settings.validate_email_config():
            print("Warning: Email configuration incomplete. Skipping email notification.")
            return
        
        try:
            subject = f"✓ ETL Pipeline Success - {execution_date.strftime('%Y-%m-%d')}"
            body = self._create_success_email_body(stats, execution_date)
            
            message = MIMEMultipart()
            message["From"] = self.email_user
            message["To"] = self.email_recipient
            message["Subject"] = subject
            
            message.attach(MIMEText(body, "plain"))
            
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.email_user, self.email_password)
                server.send_message(message)
            
            print(f"Success notification email sent to {self.email_recipient}")
            
        except Exception as e:
            print(f"Failed to send success email: {str(e)}")
    
    def send_error_alert(self, error_message: str, execution_date: datetime) -> None:
        """
        Send an error alert email.
        
        Args:
            error_message: The error message to include in the email
            execution_date: Date of the failed execution
            
        Raises:
            Exception: If email sending fails
        """
        if not settings.validate_email_config():
            print("Warning: Email configuration incomplete. Skipping email notification.")
            return
        
        try:
            subject = f"✗ ETL Pipeline Error - {execution_date.strftime('%Y-%m-%d')}"
            body = self._create_error_email_body(error_message, execution_date)
            
            message = MIMEMultipart()
            message["From"] = self.email_user
            message["To"] = self.email_recipient
            message["Subject"] = subject
            
            message.attach(MIMEText(body, "plain"))
            
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.email_user, self.email_password)
                server.send_message(message)
            
            print(f"Error alert email sent to {self.email_recipient}")
            
        except Exception as e:
            print(f"Failed to send error email: {str(e)}")
            raise Exception(f"Email notification failed: {str(e)}")
    
    def _create_success_email_body(self, stats: dict, execution_date: datetime) -> str:
        """
        Create formatted email body for success notification.
        
        Args:
            stats: Dictionary with execution statistics
            execution_date: Date of execution
            
        Returns:
            Formatted email body
        """
        body = f"""
ETL Pipeline Execution Completed Successfully

Date: {execution_date.strftime('%Y-%m-%d %H:%M:%S')}
Project: AppASO

Execution Summary:
------------------
Keywords processed: {stats.get('keywords', 0)} rows
Installs processed: {stats.get('installs', 0)} rows
Users processed: {stats.get('users', 0)} rows

Data updated in Google Sheets: MASTER_DATA_CLEAN
Local backup saved to: data/processed/

Next scheduled execution: Tomorrow at 9:00 AM Paris time

This is an automated message from the AppASO ETL Pipeline.
        """
        return body.strip()
    
    def _create_error_email_body(self, error_message: str, execution_date: datetime) -> str:
        """
        Create formatted email body for error notification.
        
        Args:
            error_message: The error message
            execution_date: Date of execution
            
        Returns:
            Formatted email body
        """
        body = f"""
ETL Pipeline Execution Failed

Date: {execution_date.strftime('%Y-%m-%d %H:%M:%S')}
Project: AppASO

Error Details:
{error_message}

Please check the logs in Google Drive folder: 03_Logs_And_Errors

This is an automated message from the AppASO ETL Pipeline.
        """
        return body.strip()
