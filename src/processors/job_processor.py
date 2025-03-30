import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List

import requests

from src.models.models import Job, UserLimits, RiskReport
from src.models.event_mapper import EventMapper, EventType
from src.config.config import Config
from src.analyzers.overtrading_analyzer import OvertradingAnalyzer
from src.utils import log_util

logger = log_util.get_logger()

# todo refactor mapping, doesnt belong here
class JobProcessor:
    def __init__(self):
        """Initialize the job processor"""
        self.overtrading_analyzer = OvertradingAnalyzer()
        self.user_limits_cache = {}  # todo normal caching? Streamlining into another topic?
        logger.info(f"API base URL: {Config.API_BASE_URL}")

    def _fetch_user_limits(self, user_id: int) -> Optional[UserLimits]:
        """Fetch user limits from the main server"""
        if user_id in self.user_limits_cache:
            return self.user_limits_cache[user_id]

        try:
            logger.info(f"Fetching limits for user {user_id}")
            response = requests.get(Config.get_user_limits_url(user_id))
            response.raise_for_status()
            limits = UserLimits(**response.json())
            self.user_limits_cache[user_id] = limits
            logger.info(f"Successfully fetched limits for user {user_id}")
            return limits
        except requests.RequestException as e:
            logger.error(f"Error fetching user limits: {e}")
            return None

    def update_job_state(self,
                         job_data: Dict[str, Any],
                         jobs_state: Dict[int, Dict[str, Any]],
                         dca_jobs: Dict[int, Dict[str, Any]],
                         liq_jobs: Dict[int, Dict[str, Any]],
                         job_to_user_map: Dict[int, int] = None,
                         is_historical: bool = False) -> None:
        """
        Update the internal state with the job event data. 
        Focuses on state management, relying on EventMapper for data normalization.

        Args:
            job_data: The job event data
            jobs_state: Dictionary mapping user_id to dictionary of job_id -> job_data
            dca_jobs: Dictionary mapping user_id to dictionary of job_id -> job_data for DCA jobs
            liq_jobs: Dictionary mapping user_id to dictionary of job_id -> job_data for liquidity jobs
            job_to_user_map: Mapping of job_id to user_id for quick lookups
            is_historical: Whether this is a historical event (for initial loading)
        """
        try:
            # Get standardized event data from the mapper
            mapped_data = EventMapper.map_job_data(job_data)
            event_type = mapped_data.get('event_type')
            
            job_id = mapped_data.get('job_id')
            user_id = mapped_data.get('user_id')
            job_name = mapped_data.get('name', '').lower()

            if not job_id:
                logger.warning(f"Missing job_id in job data: {job_data}")
                return

            # For updates without user_id, try to find the job via job_to_user_map
            if not user_id and job_to_user_map and job_id in job_to_user_map:
                user_id = job_to_user_map[job_id]
                # Add the user_id to mapped_data for consistency
                mapped_data['user_id'] = user_id
                if not is_historical:
                    logger.info(f"Found user_id {user_id} for job {job_id} in job_to_user_map")
            elif not user_id:
                logger.warning(f"Cannot find user_id for job {job_id} in job_to_user_map")
                return

            # Initialize user state if needed
            if user_id not in jobs_state:
                jobs_state[user_id] = {}
                if not is_historical:
                    logger.info(f"Created new state for user {user_id}")

            # STATE MANAGEMENT: Determine if this is a new job or an update
            is_new_job = job_id not in jobs_state[user_id]
            
            # Process according to whether it's a new job or update
            if is_new_job:
                self._create_new_job(
                    user_id, job_id, mapped_data, 
                    jobs_state, dca_jobs, liq_jobs, 
                    is_historical
                )
            else:
                self._update_existing_job(
                    user_id, job_id, mapped_data, 
                    jobs_state, dca_jobs, liq_jobs, 
                    is_historical
                )
                
        except Exception as e:
            job_id = job_data.get('job_id', 'unknown')
            logger.error(f"Error updating job state for job {job_id}: {e}", exc_info=True)
    
    def _create_new_job(self, 
                       user_id: int, 
                       job_id: int,
                       job_data: Dict[str, Any],
                       jobs_state: Dict[int, Dict[str, Any]],
                       dca_jobs: Dict[int, Dict[str, Any]],
                       liq_jobs: Dict[int, Dict[str, Any]],
                       is_historical: bool = False) -> None:
        """
        Create a new job in the state collections.
        
        Args:
            user_id: The user ID
            job_id: The job ID
            job_data: The mapped job data 
            jobs_state: Dictionary mapping user_id to dictionary of job_id -> job_data
            dca_jobs: Dictionary mapping user_id to dictionary of job_id -> job_data for DCA jobs
            liq_jobs: Dictionary mapping user_id to dictionary of job_id -> job_data for liquidity jobs
            is_historical: Whether this is a historical event
        """
        job_name = job_data.get('name', '').lower()
        jobs_state[user_id][job_id] = job_data
        
        if not is_historical:
            job_status = job_data.get('status', 'unknown')
            logger.info(f"Created new job {job_id} for user {user_id}, name: '{job_name}', status: {job_status}")
        
        # Add to appropriate strategy collection if it's a specialized job type
        if job_name in ['dca', 'Dca', 'DCA']:
            if user_id not in dca_jobs:
                dca_jobs[user_id] = {}
            dca_jobs[user_id][job_id] = job_data
            if not is_historical:
                logger.info(f"Added new job {job_id} to DCA jobs collection")
            else:
                logger.debug(f"Added historical job {job_id} to DCA jobs collection")
        elif job_name in ['liq', 'Liq', 'LIQ']:
            if user_id not in liq_jobs:
                liq_jobs[user_id] = {}
            liq_jobs[user_id][job_id] = job_data
            if not is_historical:
                logger.info(f"Added new job {job_id} to LIQ jobs collection")
            else:
                logger.debug(f"Added historical job {job_id} to LIQ jobs collection")
    
    def _update_existing_job(self, 
                            user_id: int, 
                            job_id: int,
                            update_data: Dict[str, Any],
                            jobs_state: Dict[int, Dict[str, Any]],
                            dca_jobs: Dict[int, Dict[str, Any]],
                            liq_jobs: Dict[int, Dict[str, Any]],
                            is_historical: bool = False) -> None:
        """
        Update an existing job in the state collections.
        
        Args:
            user_id: The user ID
            job_id: The job ID
            update_data: The mapped job update data
            jobs_state: Dictionary mapping user_id to dictionary of job_id -> job_data
            dca_jobs: Dictionary mapping user_id to dictionary of job_id -> job_data for DCA jobs
            liq_jobs: Dictionary mapping user_id to dictionary of job_id -> job_data for liquidity jobs
            is_historical: Whether this is a historical event
        """
        existing_job = jobs_state[user_id][job_id]
        job_name = existing_job.get('name', '').lower()
        
        # Log the current state before updating
        if not is_historical:
            old_status = existing_job.get('status', 'unknown')
            old_steps = existing_job.get('completed_steps', 0)
            old_orders_count = len(existing_job.get('orders', []))
            logger.info(
                f"Existing job {job_id} status: {old_status}, steps: {old_steps}, orders: {old_orders_count}")
        
        # Update fields based on the event data
        updated_fields = []
        for field in ['status', 'completed_steps', 'orders']:
            if field in update_data and update_data[field] is not None:
                # Track what we're updating for logging
                if field == 'orders':
                    updated_fields.append(f"{field}={len(update_data[field])}")
                else:
                    updated_fields.append(f"{field}={update_data[field]}")
                    
                # Apply the update
                existing_job[field] = update_data[field]
        
        # Log updates
        if not is_historical and updated_fields:
            logger.info(f"Updated job {job_id} fields: {', '.join(updated_fields)}")
        
        # Sync changes to appropriate strategy collections
        self._sync_to_strategy_collections(user_id, job_id, existing_job, job_name, dca_jobs, liq_jobs, is_historical)
    
    def _sync_to_strategy_collections(self,
                                     user_id: int,
                                     job_id: int,
                                     job_data: Dict[str, Any],
                                     job_name: str,
                                     dca_jobs: Dict[int, Dict[str, Any]],
                                     liq_jobs: Dict[int, Dict[str, Any]],
                                     is_historical: bool = False) -> None:
        """
        Sync job updates to the appropriate strategy collections.
        
        Args:
            user_id: The user ID 
            job_id: The job ID
            job_data: The job data to sync
            job_name: The job name/type
            dca_jobs: Dictionary mapping user_id to dictionary of job_id -> job_data for DCA jobs
            liq_jobs: Dictionary mapping user_id to dictionary of job_id -> job_data for liquidity jobs
            is_historical: Whether this is a historical event
        """
        if job_name in ['dca', 'Dca', 'DCA'] and user_id in dca_jobs and job_id in dca_jobs[user_id]:
            dca_jobs[user_id][job_id] = job_data
            if not is_historical:
                logger.debug(f"Synced job {job_id} updates to DCA collection")
        elif job_name in ['liq', 'Liq', 'LIQ'] and user_id in liq_jobs and job_id in liq_jobs[user_id]:
            liq_jobs[user_id][job_id] = job_data
            if not is_historical:
                logger.debug(f"Synced job {job_id} updates to LIQ collection")
    
    def analyze_risk(self, job_data: Dict[str, Any], jobs_state: Dict[int, Dict[str, Any]],
                    dca_jobs: Dict[int, Dict[str, Any]], liq_jobs: Dict[int, Dict[str, Any]]) -> Optional[RiskReport]:
        """
        Analyze job data for risk factors

        Args:
            job_data: The job data to analyze
            jobs_state: Current state of all jobs
            dca_jobs: Dictionary of DCA jobs
            liq_jobs: Dictionary of LIQ jobs

        Returns:
            RiskReport if risks detected, None otherwise
        """
        try:
            job_id = job_data.get('job_id')
            user_id = job_data.get('user_id')

            if not user_id:
                logger.warning(f"Skipping risk analysis: Missing user_id for job {job_id}")
                return None

            if not job_id:
                logger.warning(f"Skipping risk analysis: Missing job_id for user {user_id}")
                return None

            # Log start of risk analysis
            logger.debug(f"Starting risk analysis for job {job_id} (user {user_id})")

            # Get the job's data using our mapper
            job = EventMapper.map_to_job(job_data)

            # Get user's trading limits
            user_limits = self._fetch_user_limits(user_id)
            logger.info(f"User {user_id} limits: max_position={user_limits.max_position_size}, "
                        f"max_daily_trades={user_limits.max_daily_trades}, "
                        f"max_daily_volume={user_limits.max_daily_volume}, "
                        f"max_concurrent={user_limits.max_concurrent_jobs}, "
                        f"cooldown={user_limits.min_trade_interval_minutes}min")

            # Get all active jobs for the user
            all_user_jobs = []
            if user_id in jobs_state:
                user_job_map = jobs_state[user_id]
                for existing_job_id, existing_job_data in user_job_map.items():
                    if existing_job_data:  # Skip None values
                        try:
                            # Create Job object from each job's data
                            job_obj = Job(**existing_job_data)
                            all_user_jobs.append(job_obj)
                        except Exception as e:
                            logger.error(f"Error converting job {existing_job_id} to Job object: {e}")

            # Count active jobs for user
            active_jobs = [j for j in all_user_jobs
                           if j.status not in ['Finished', 'Stopped']]
            logger.debug(
                f"Found {len(active_jobs)} active jobs out of {len(all_user_jobs)} total jobs for user {user_id}")

            # Include the current job if it's not in the jobs_state yet
            current_job_in_state = False
            if user_id in jobs_state:
                current_job_in_state = job_id in jobs_state[user_id]

            if not current_job_in_state:
                logger.info(f"Adding current job {job_id} to analysis since it's not in state yet")
                all_user_jobs.append(job)

            logger.info(f"Analyzing overtrading risks for job {job_id}")
            risk_report = self.overtrading_analyzer.analyze(job, all_user_jobs, user_limits)

            if risk_report and risk_report.triggers:
                logger.warning(
                    f"Risk report generated with {len(risk_report.triggers)} triggers and risk level {risk_report.level}")
                # Log each trigger
                for i, trigger in enumerate(risk_report.triggers):
                    logger.warning(f"Risk trigger {i + 1}: {trigger['message']} - {trigger['details']}")
                return risk_report
            else:
                logger.info(f"No risk triggers found for job {job_id}")
                return None

        except Exception as e:
            logger.exception(f"Error analyzing risk: {e}")
            return None

