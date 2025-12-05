"""
Utilities package for common functionality.
"""

from .notifications import EmailService
from .error_handler import ErrorHandler

__all__ = ["EmailService", "ErrorHandler"]
