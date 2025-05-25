"""
Utility functions for datetime operations.
"""

from datetime import datetime, timezone
from typing import Optional, Union


def parse_timestamp(timestamp: Union[str, float, int, datetime]) -> datetime:
    """
    Parse a timestamp into a timezone-aware datetime object.
    Optimized for Kafka-style timestamps (e.g., "2025-04-06T19:58:33.305362822Z").
    
    Args:
        timestamp: String timestamp, epoch timestamp, or datetime object
        
    Returns:
        Timezone-aware datetime object (UTC)
        
    Raises:
        ValueError: If the timestamp cannot be parsed
    """
    # If already a datetime, ensure it has timezone
    if isinstance(timestamp, datetime):
        return timestamp if timestamp.tzinfo else timestamp.replace(tzinfo=timezone.utc)
        
    # If numeric, treat as epoch timestamp (UTC)
    if isinstance(timestamp, (int, float)):
        return datetime.fromtimestamp(timestamp, tz=timezone.utc)
        
    # Handle string timestamps
    if isinstance(timestamp, str):
        # Handle Kafka-style timestamps (with Z for UTC)
        if 'Z' in timestamp:
            # Remove the 'Z' and parse
            ts = timestamp.rstrip('Z')
            
            # Handle precision beyond microseconds
            if '.' in ts:
                parts = ts.split('.')
                # Python only handles microseconds (6 digits)
                if len(parts[1]) > 6:
                    ts = f"{parts[0]}.{parts[1][:6]}"
            
            # Parse and add UTC timezone
            try:
                dt = datetime.fromisoformat(ts)
                return dt.replace(tzinfo=timezone.utc)
            except ValueError:
                pass  # Try other formats
                
        # Try ISO 8601 with timezone
        try:
            return datetime.fromisoformat(timestamp)
        except ValueError:
            pass
            
        # Try ISO without timezone (assume UTC)
        try:
            dt = DateTimeUtils.parse_timestamp(timestamp)
            if dt:
                return dt.replace(tzinfo=timezone.utc)
        except:
            pass
    
    # If all parsing attempts fail
    raise ValueError(f"Could not parse timestamp: {timestamp}")


def format_timestamp(dt: datetime) -> str:
    """
    Format a datetime to Kafka-compatible ISO 8601 string with Z timezone.
    
    Args:
        dt: Datetime object to format
        
    Returns:
        Formatted string like "2025-04-06T19:58:33.305Z"
    """
    # Ensure datetime is in UTC
    dt_utc = dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    
    # Format with millisecond precision and Z timezone
    formatted = dt_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    return formatted


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
            Timezone-aware datetime object (UTC) or None if parsing fails
        """
        if not timestamp:
            return None
            
        # If numeric, treat as epoch timestamp
        if isinstance(timestamp, (int, float)):
            try:
                return datetime.fromtimestamp(timestamp, tz=timezone.utc)
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
                    
                    dt = datetime.strptime(timestamp, cls.ISO_FORMAT)
                    return dt.replace(tzinfo=timezone.utc)
                else:
                    # No microseconds
                    # Remove timezone if present
                    if 'Z' in timestamp:
                        timestamp = timestamp.split('Z')[0]
                    if '+' in timestamp:
                        timestamp = timestamp.split('+')[0]
                    
                    dt = datetime.strptime(timestamp, cls.ISO_FORMAT_NO_MS)
                    return dt.replace(tzinfo=timezone.utc)
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
                    dt = datetime.strptime(timestamp, fmt)
                    return dt.replace(tzinfo=timezone.utc)
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