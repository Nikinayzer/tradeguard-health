"""
Event Mapper Utility

Provides mapping functionality between raw event data and model instances.
"""

from typing import Dict, Any, Optional, List, Union
from datetime import datetime
from enum import Enum

from src.models.job_models import Job, JobEvent, CreateJobEvent
from src.utils import log_util

logger = log_util.get_logger()


class EventType(Enum):
    """Enum representing the different types of job events"""
    CREATED = "Created"
    UPDATED = "Updated"
    PAUSED = "Paused"
    RESUMED = "Resumed"
    STOPPED = "Stopped"
    FINISHED = "Finished"
    STEP_DONE = "StepDone"
    ORDERS_PLACED = "OrdersPlaced"
    UNKNOWN = "Unknown"


class EventMapper:
    """
    Utility class for mapping event data to model instances.
    Responsible for standardizing data formats, not for state management.
    """

    @staticmethod
    def get_event_type(event_data: Dict[str, Any]) -> EventType:
        """
        Determine the event type from event data.
        
        Args:
            event_data: Raw dictionary from Kafka event
            
        Returns:
            Standardized EventType
        """
        event_type = event_data.get('event_type')

        # Handle string event types
        if isinstance(event_type, str):
            try:
                return EventType(event_type)
            except ValueError:
                logger.debug(f"Unknown event type string: {event_type}")
                return EventType.UNKNOWN

        # Handle dictionary event types
        elif isinstance(event_type, dict):
            if 'Created' in event_type:
                return EventType.CREATED
            elif 'StepDone' in event_type:
                return EventType.STEP_DONE
            elif 'OrdersPlaced' in event_type:
                return EventType.ORDERS_PLACED

        # Default case
        logger.warning(f"Unrecognized event type format: {event_type}")
        return EventType.UNKNOWN

    @staticmethod
    def map_job_event(event_data: Dict[str, Any]) -> JobEvent:
        """
        Map raw event data to a JobEvent or specialized subclass.
        
        Args:
            event_data: Raw dictionary from Kafka event
            
        Returns:
            Appropriate JobEvent instance
        """
        try:
            event_type = event_data.get('event_type')

            # For Created events, use the specialized class
            if event_type == 'Created':
                return CreateJobEvent(**event_data)

            # For other events, use the base class
            return JobEvent(**event_data)

        except Exception as e:
            logger.warning(f"Error mapping job event: {e}. Using generic JobEvent.")
            # Ensure we at least have the minimum fields
            safe_data = {
                'job_id': event_data.get('job_id', 0),
                'user_id': event_data.get('user_id'),
                'event_type': event_data.get('event_type', 'Unknown'),
                'timestamp': event_data.get('timestamp', datetime.now().isoformat())
            }
            return JobEvent(**safe_data)

    @staticmethod
    def map_job_data(job_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Map incoming job data to a standardized dictionary format.
        Focus on formatting and normalization, not state management.
        
        Args:
            job_data: Raw dictionary from Kafka event
            
        Returns:
            Standardized dictionary with all available fields
        """
        job_id = job_data.get('job_id')
        event_type = EventMapper.get_event_type(job_data)

        # Log key fields for debugging
        logger.debug(f"Mapping job data: job_id={job_id}, event_type={event_type}, has_user_id={'user_id' in job_data}")

        # Validate job_id is present (critical for mapping)
        if not job_id:
            logger.error(f"Missing job_id in job data: {job_data}")

        # For 'Created' events, validate user_id is present
        if event_type == EventType.CREATED and 'user_id' not in job_data:
            logger.error(f"Missing user_id in 'Created' event for job {job_id}. This will cause mapping issues.")

        # Base fields that should be present in all job events
        mapped_data = {
            'job_id': job_id,
            'event_type': event_type.value,
            'timestamp': job_data.get('timestamp', datetime.now().isoformat()),
        }

        # Include user_id if present
        if 'user_id' in job_data:
            mapped_data['user_id'] = job_data['user_id']

        # Add all fields that might be present in the event
        for field in [
            'name', 'coins', 'side', 'discount_pct', 'amount', 'steps_total',
            'completed_steps', 'duration_minutes', 'orders', 'status'
        ]:
            if field in job_data:
                mapped_data[field] = job_data[field]

        # Handle special fields based on event type
        raw_event_type = job_data.get('event_type')

        # Set default status based on event type if not specified
        if 'status' not in mapped_data:
            if event_type == EventType.CREATED:
                mapped_data['status'] = 'Created'
            elif event_type in [EventType.RESUMED, EventType.STEP_DONE, EventType.ORDERS_PLACED]:
                mapped_data['status'] = 'Running'
            elif event_type == EventType.PAUSED:
                mapped_data['status'] = 'Paused'
            elif event_type == EventType.STOPPED:
                mapped_data['status'] = 'Stopped'
            elif event_type == EventType.FINISHED:
                mapped_data['status'] = 'Finished'

        # Handle complex event types (dictionary with event details)
        if isinstance(raw_event_type, dict):
            logger.debug(f"Processing complex event type: {raw_event_type}")

            # Extract data from Created events
            if 'Created' in raw_event_type:
                created_data = raw_event_type['Created']
                for key, value in created_data.items():
                    mapped_data[key] = value

            # Extract data from StepDone events
            elif 'StepDone' in raw_event_type:
                mapped_data['completed_steps'] = raw_event_type['StepDone']

            # Extract data from OrdersPlaced events
            elif 'OrdersPlaced' in raw_event_type:
                mapped_data['orders'] = raw_event_type['OrdersPlaced']

        # Set default values for new jobs if they don't exist
        if event_type == EventType.CREATED:
            if 'completed_steps' not in mapped_data:
                mapped_data['completed_steps'] = 0
            if 'orders' not in mapped_data:
                mapped_data['orders'] = []

        logger.debug(f"Mapped job data (event_type={event_type.value}): {list(mapped_data.keys())}")
        return mapped_data

    @staticmethod
    def map_to_job(event_data: Dict[str, Any]) -> Job:
        """
        Map raw event data to a Job instance.
        
        Args:
            event_data: Raw dictionary from Kafka event
            
        Returns:
            Job instance
        """
        mapped_data = EventMapper.map_job_data(event_data)
        return Job(**mapped_data)
