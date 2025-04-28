"""
Job Storage Manager

Provides in-memory storage for job data.
Handles job storage, retrieval, and categorization by type.
"""

from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta, timezone
from threading import Lock

from src.models.job_models import Job
from src.utils.log_util import get_logger

logger = get_logger()


class JobStorage:
    """
    Storage manager for job data with in-memory storage.
    Provides methods to store, retrieve, and manage job data.
    """

    def __init__(self):
        """
        Initialize the job storage with in-memory storage.
        """
        # In-memory storage
        self._jobs_state = {}  # user_id -> job_id -> Job
        self._dca_jobs = {}  # user_id -> job_id -> Job
        self._liq_jobs = {}  # user_id -> job_id -> Job
        self._job_to_user_map = {}  # job_id -> user_id

        self._lock = Lock()

        logger.info("Job storage initialized with in-memory storage only")

    def store_job(self, job: Job) -> None:
        """
        Store a job with appropriate indexing.
        
        Args:
            job: Job object to store
        """
        self._store_job_in_memory(job)

    def _store_job_in_memory(self, job: Job) -> None:
        """Store job in memory."""
        with self._lock:
            user_id = job.user_id
            job_id = job.job_id

            # Store the mapping
            self._job_to_user_map[job_id] = user_id

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
            
            logger.debug(f"Stored job {job_id} for user {user_id} in memory (job status: {job.status})")

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

    def _filter_jobs_by_timeframe(self, jobs: Dict[int, Job], hours: int) -> Dict[int, Job]:
        """
        Filter jobs by timeframe.
        
        Args:
            jobs: Dictionary of jobs to filter
            hours: Number of hours to look back. If 0 or negative, returns all jobs.
            
        Returns:
            Dictionary of jobs within the specified timeframe
        """
        if hours <= 0 or not jobs:
            return jobs.copy()

        # Calculate cutoff time in UTC
        current_time = datetime.now(timezone.utc)
        cutoff_time = current_time - timedelta(hours=hours)

        # Filter jobs by timestamp
        recent_jobs = {
            job_id: job for job_id, job in jobs.items()
            if job.timestamp >= cutoff_time or hours >= 720  # Default to 30 days if a large window is requested
        }

        return recent_jobs

    def get_user_jobs(self, user_id: int, hours: int = 0) -> Dict[int, Job]:
        """
        Get all jobs for a specific user within a specified timeframe.
        
        Args:
            user_id: The ID of the user
            hours: Number of hours to look back (default: 0, meaning all jobs)
            
        Returns:
            Dictionary of jobs for the user within the specified timeframe
        """
        with self._lock:
            if user_id not in self._jobs_state:
                return {}
                
            return self._filter_jobs_by_timeframe(self._jobs_state[user_id], hours)

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

    def get_jobs_state(self, hours: int = 0) -> Dict[int, Dict[int, Job]]:
        """
        Get a copy of the entire jobs state, optionally filtered by timeframe.
        
        Args:
            hours: Number of hours to look back (default: 0, meaning all jobs)
            
        Returns:
            Dictionary mapping user IDs to their job histories
        """
        with self._lock:
            if hours <= 0:
                return {user_id: jobs.copy() for user_id, jobs in self._jobs_state.items()}

            # Filter each user's jobs by timeframe
            filtered_state = {}
            for user_id, jobs in self._jobs_state.items():
                filtered_jobs = self._filter_jobs_by_timeframe(jobs, hours)
                if filtered_jobs:  # Only include users with recent jobs
                    filtered_state[user_id] = filtered_jobs

            # Log the total number of jobs before and after filtering
            total_before = sum(len(jobs) for jobs in self._jobs_state.values())
            total_after = sum(len(jobs) for jobs in filtered_state.values())
            logger.debug(f"get_jobs_state: {total_before} total jobs, {total_after} after {hours}h filtering")
            
            return filtered_state

    def get_dca_jobs(self, hours: int = 0) -> Dict[int, Dict[int, Job]]:
        """
        Get a copy of the DCA jobs state, optionally filtered by timeframe.
        
        Args:
            hours: Number of hours to look back (default: 0, meaning all jobs)
            
        Returns:
            Dictionary mapping user IDs to their DCA jobs
        """
        with self._lock:
            if hours <= 0:
                return {user_id: jobs.copy() for user_id, jobs in self._dca_jobs.items()}
                
            # Filter each user's DCA jobs by timeframe
            filtered_state = {}
            for user_id, jobs in self._dca_jobs.items():
                filtered_jobs = self._filter_jobs_by_timeframe(jobs, hours)
                if filtered_jobs:
                    filtered_state[user_id] = filtered_jobs
            
            # Log the total number of DCA jobs before and after filtering
            total_before = sum(len(jobs) for jobs in self._dca_jobs.values())
            total_after = sum(len(jobs) for jobs in filtered_state.values())
            logger.debug(f"get_dca_jobs: {total_before} total DCA jobs, {total_after} after {hours}h filtering")
                    
            return filtered_state

    def get_liq_jobs(self, hours: int = 0) -> Dict[int, Dict[int, Job]]:
        """
        Get a copy of the LIQ jobs state, optionally filtered by timeframe.
        
        Args:
            hours: Number of hours to look back (default: 0, meaning all jobs)
            
        Returns:
            Dictionary mapping user IDs to their LIQ jobs
        """
        with self._lock:
            if hours <= 0:
                return {user_id: jobs.copy() for user_id, jobs in self._liq_jobs.items()}
                
            # Filter each user's LIQ jobs by timeframe
            filtered_state = {}
            for user_id, jobs in self._liq_jobs.items():
                filtered_jobs = self._filter_jobs_by_timeframe(jobs, hours)
                if filtered_jobs:
                    filtered_state[user_id] = filtered_jobs
            
            # Log the total number of LIQ jobs before and after filtering
            total_before = sum(len(jobs) for jobs in self._liq_jobs.values())
            total_after = sum(len(jobs) for jobs in filtered_state.values())
            logger.debug(f"get_liq_jobs: {total_before} total LIQ jobs, {total_after} after {hours}h filtering")
                    
            return filtered_state

    def get_job_to_user_map(self) -> Dict[int, int]:
        """
        Get a copy of the job_id to user_id mapping.
        
        Returns:
            Dictionary mapping job IDs to user IDs
        """
        with self._lock:
            return self._job_to_user_map.copy()

    def clear_job_data(self, user_id: Optional[int] = None) -> None:
        """
        Clear job data, optionally filtered by user.
        
        Args:
            user_id: Optional user ID to clear data for
        """
        with self._lock:
            if user_id:
                # Clear specific user's jobs
                if user_id in self._jobs_state:
                    # Get list of job IDs to remove from the map
                    job_ids = list(self._jobs_state[user_id].keys())
                    
                    # Remove jobs from job-to-user map
                    for job_id in job_ids:
                        if job_id in self._job_to_user_map:
                            del self._job_to_user_map[job_id]
                            
                    # Clear user's job states
                    del self._jobs_state[user_id]
                    
                # Clear DCA jobs
                if user_id in self._dca_jobs:
                    del self._dca_jobs[user_id]
                    
                # Clear LIQ jobs
                if user_id in self._liq_jobs:
                    del self._liq_jobs[user_id]
            else:
                # Clear all job data
                self._jobs_state.clear()
                self._dca_jobs.clear()
                self._liq_jobs.clear()
                self._job_to_user_map.clear()

    def clear_all_job_data(self) -> None:
        """Clear all job data."""
        self.clear_job_data()
