"""
State Management Service

Manages shared application state including job histories and user mappings.
Provides thread-safe access to state and handles state updates from Kafka events.
"""

import logging
from typing import Dict, Any, Optional
from threading import Lock
from datetime import datetime

from src.models.job_models import Job
from src.utils.log_util import get_logger

logger = get_logger()


class StateManager:
    """
    Manages shared application state with thread-safe access.
    Provides simple storage and retrieval of jobs.
    """

    def __init__(self):
        """Initialize the state manager with empty state."""
        self._jobs_state: Dict[int, Dict[int, Job]] = {}
        self._dca_jobs: Dict[int, Dict[int, Job]] = {}
        self._liq_jobs: Dict[int, Dict[int, Job]] = {}
        self._job_to_user_map: Dict[int, int] = {}
        self._lock = Lock()
        logger.info("State manager initialized")

    def store_job(self, job: Job) -> None:
        """
        Store a job in the state.
        
        Args:
            job: The job to store
        """
        with self._lock:
            try:
                user_id = job.user_id
                job_id = job.job_id
                
                # Store the mapping
                self._job_to_user_map[job_id] = user_id
                logger.debug(f"Created mapping: job {job_id} -> user {user_id}")

                # Initialize user's state if needed
                if user_id not in self._jobs_state:
                    self._jobs_state[user_id] = {}

                # Store the job instance
                self._jobs_state[user_id][job_id] = job

                # Categorize job by type
                if job.is_dca_job:
                    if user_id not in self._dca_jobs:
                        self._dca_jobs[user_id] = {}
                    self._dca_jobs[user_id][job_id] = job
                elif job.is_liq_job:
                    if user_id not in self._liq_jobs:
                        self._liq_jobs[user_id] = {}
                    self._liq_jobs[user_id][job_id] = job
                    
            except Exception as e:
                logger.error(f"Error storing job: {e}")

    def get_job(self, job_id: int) -> Optional[Job]:
        """
        Get a job by its ID.
        
        Args:
            job_id: The ID of the job
            
        Returns:
            The job if found, None otherwise
        """
        with self._lock:
            user_id = self._job_to_user_map.get(job_id)
            if not user_id:
                return None
            return self._jobs_state.get(user_id, {}).get(job_id)

    def get_user_jobs(self, user_id: int) -> Dict[int, Job]:
        """
        Get all jobs for a specific user.
        
        Args:
            user_id: The ID of the user
            
        Returns:
            Dictionary of jobs for the user
        """
        with self._lock:
            return self._jobs_state.get(user_id, {}).copy()

    def get_job_user(self, job_id: int) -> Optional[int]:
        """
        Get the user ID for a specific job.
        
        Args:
            job_id: The ID of the job
            
        Returns:
            User ID if found, None otherwise
        """
        with self._lock:
            return self._job_to_user_map.get(job_id)

    def get_jobs_state(self) -> Dict[int, Dict[int, Job]]:
        """
        Get a copy of the entire jobs state.
        
        Returns:
            Dictionary mapping user IDs to their job histories
        """
        with self._lock:
            return {user_id: jobs.copy() for user_id, jobs in self._jobs_state.items()}

    def get_dca_jobs(self) -> Dict[int, Dict[int, Job]]:
        """
        Get a copy of the DCA jobs state.
        
        Returns:
            Dictionary mapping user IDs to their DCA jobs
        """
        with self._lock:
            return {user_id: jobs.copy() for user_id, jobs in self._dca_jobs.items()}

    def get_liq_jobs(self) -> Dict[int, Dict[int, Job]]:
        """
        Get a copy of the LIQ jobs state.
        
        Returns:
            Dictionary mapping user IDs to their LIQ jobs
        """
        with self._lock:
            return {user_id: jobs.copy() for user_id, jobs in self._liq_jobs.items()}

    def get_job_to_user_map(self) -> Dict[int, int]:
        """
        Get a copy of the job-to-user mapping.
        
        Returns:
            Dictionary mapping job IDs to user IDs
        """
        with self._lock:
            return self._job_to_user_map.copy()

    def clear(self) -> None:
        """Clear all state data."""
        with self._lock:
            self._jobs_state.clear()
            self._dca_jobs.clear()
            self._liq_jobs.clear()
            self._job_to_user_map.clear()
            logger.info("State cleared")
