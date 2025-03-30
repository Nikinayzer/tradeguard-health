"""
Utility functions for datetime operations.
"""

from datetime import datetime
from typing import Optional, Union

class DateTimeUtils:
    """Utility class for datetime operations."""
    
    ISO_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"
    ISO_FORMAT_NO_MS = "%Y-%m-%dT%H:%M:%S"
    
    @classmethod
    def parse_timestamp(cls, timestamp: Union[str, float, int]) -> Optional[datetime]:
        """
        Parse a timestamp string or numeric value into a datetime object.
        
        Args:
            timestamp: A string timestamp (ISO format preferred) or epoch timestamp
            
        Returns:
            datetime object or None if parsing fails
        """
        if not timestamp:
            return None
            
        # If numeric, treat as epoch timestamp
        if isinstance(timestamp, (int, float)):
            try:
                return datetime.fromtimestamp(timestamp)
            except (ValueError, OverflowError, OSError):
                return None
                
        # If string, try various formats
        if isinstance(timestamp, str):
            # Try ISO format with microseconds
            try:
                if '.' in timestamp:  # Has microseconds
                    # Handle varying precision in microseconds by standardizing
                    parts = timestamp.split('.')
                    if len(parts) == 2:
                        timestamp_base = parts[0]
                        # Standardize microseconds to 6 digits
                        micro = parts[1]
                        if 'Z' in micro:  # Handle UTC marker
                            micro = micro.split('Z')[0]
                        if '+' in micro:  # Handle timezone
                            micro = micro.split('+')[0]
                        if len(micro) > 6:
                            micro = micro[:6]
                        elif len(micro) < 6:
                            micro = micro.ljust(6, '0')
                        timestamp = f"{timestamp_base}.{micro}"
                    
                    return datetime.strptime(timestamp, cls.ISO_FORMAT)
                else:
                    # No microseconds
                    # Remove timezone if present
                    if 'Z' in timestamp:
                        timestamp = timestamp.split('Z')[0]
                    if '+' in timestamp:
                        timestamp = timestamp.split('+')[0]
                    
                    return datetime.strptime(timestamp, cls.ISO_FORMAT_NO_MS)
            except ValueError:
                pass
                
            # Try common date formats
            formats = [
                "%Y-%m-%d %H:%M:%S",
                "%Y/%m/%d %H:%M:%S",
                "%d-%m-%Y %H:%M:%S",
                "%d/%m/%Y %H:%M:%S",
                "%Y-%m-%d",
                "%Y/%m/%d",
                "%d-%m-%Y",
                "%d/%m/%Y"
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(timestamp, fmt)
                except ValueError:
                    continue
                    
        # If all parsing attempts fail
        return None
        
    @classmethod
    def format_timestamp(cls, timestamp: Union[str, float, int, datetime], fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
        """
        Format a timestamp into a human-readable string.
        
        Args:
            timestamp: A datetime object, string timestamp, or epoch timestamp
            fmt: The format string to use
            
        Returns:
            Formatted string or original timestamp if formatting fails
        """
        if isinstance(timestamp, datetime):
            dt = timestamp
        else:
            dt = cls.parse_timestamp(timestamp)
            
        if dt:
            return dt.strftime(fmt)
        
        # Return original if parsing failed
        return str(timestamp) if timestamp is not None else ""
        
    @classmethod
    def get_relative_time(cls, timestamp: Union[str, float, int, datetime]) -> str:
        """
        Convert a timestamp to a human-readable relative time (e.g., "2 hours ago").
        
        Args:
            timestamp: A datetime object, string timestamp, or epoch timestamp
            
        Returns:
            Relative time string or original timestamp if parsing fails
        """
        dt = None
        if isinstance(timestamp, datetime):
            dt = timestamp
        else:
            dt = cls.parse_timestamp(timestamp)
            
        if not dt:
            return str(timestamp) if timestamp is not None else ""
            
        now = datetime.now()
        diff = now - dt
        
        # Calculate the difference
        seconds = diff.total_seconds()
        
        if seconds < 60:
            return "just now" if seconds < 10 else f"{int(seconds)} seconds ago"
        
        minutes = seconds // 60
        if minutes < 60:
            return f"{int(minutes)} minute{'s' if minutes != 1 else ''} ago"
            
        hours = minutes // 60
        if hours < 24:
            return f"{int(hours)} hour{'s' if hours != 1 else ''} ago"
            
        days = hours // 24
        if days < 30:
            return f"{int(days)} day{'s' if days != 1 else ''} ago"
            
        months = days // 30
        if months < 12:
            return f"{int(months)} month{'s' if months != 1 else ''} ago"
            
        years = months // 12
        return f"{int(years)} year{'s' if years != 1 else ''} ago" 