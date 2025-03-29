import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List

import requests

from src.models.models import Job, UserLimits, RiskReport
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

    def _map_job_data(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """Map incoming job data to match our Job model fields"""
        logger.debug(f"Received job data: {job_data}")

        job_id = job_data.get('job_id')
        event_type = job_data.get('event_type')
        name = job_data.get('name')

        # For 'Created' events -> include all fields
        # For other events -> only mutable fields
        if event_type == 'Created':
            logger.debug(f"Processing Created event for job {job_id} with name: {name}")

            mapped_data = {
                'job_id': job_data.get('job_id'),
                'user_id': job_data.get('user_id'),
                'event_type': event_type,
                'name': name,
                'coins': job_data.get('coins', []),
                'side': job_data.get('side'),
                'discount_pct': job_data.get('discount_pct', 0.0),
                'amount': job_data.get('amount', 0.0),
                'steps_total': job_data.get('steps_total', 0),
                'duration_minutes': job_data.get('duration_minutes', 0.0),
                'timestamp': job_data.get('timestamp', datetime.now().isoformat()),
                'status': 'Created',
                'completed_steps': 0,
                'orders': []
            }
        else:
            mapped_data = {
                'job_id': job_data.get('job_id'),
                'user_id': job_data.get('user_id'),
                'event_type': event_type,
                'timestamp': job_data.get('timestamp', datetime.now().isoformat()),
            }

            if 'completed_steps' in job_data:
                mapped_data['completed_steps'] = job_data['completed_steps']
            if 'orders' in job_data:
                mapped_data['orders'] = job_data['orders']

        if isinstance(event_type, dict):
            logger.debug(f"Processing complex event type: {event_type}")
            if 'Created' in event_type:
                created_data = event_type['Created']
                logger.debug(f"Processing complex Created event with data: {created_data}")

                for key in created_data:
                    if key in mapped_data:
                        mapped_data[key] = created_data[key]
                mapped_data['status'] = 'Created'

                if 'name' in created_data:
                    logger.debug(f"Job {job_id} created with name: {created_data['name']}")

            elif 'StepDone' in event_type:
                mapped_data['completed_steps'] = event_type['StepDone']
                mapped_data['status'] = 'Running'
            elif 'OrdersPlaced' in event_type:
                mapped_data['orders'] = event_type['OrdersPlaced']
                mapped_data['status'] = 'Running'

        elif isinstance(event_type, str):  # String
            logger.debug(f"Processing simple event type: {event_type}")
            if event_type == 'Paused':
                mapped_data['status'] = 'Paused'
            elif event_type == 'Resumed':
                mapped_data['status'] = 'Running'
            elif event_type == 'Stopped':
                mapped_data['status'] = 'Stopped'
            elif event_type == 'Finished':
                mapped_data['status'] = 'Finished'
            else:
                mapped_data['status'] = event_type

        logger.debug(f"Mapped job data: {mapped_data}")
        return mapped_data

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
                         is_historical: bool = False) -> None:
        """
        Update the internal state with the job event data

        Args:
            job_data: The job event data
            jobs_state: Dictionary mapping user_id to dictionary of job_id -> job_data
            dca_jobs: Dictionary mapping user_id to dictionary of job_id -> job_data for DCA jobs
            liq_jobs: Dictionary mapping user_id to dictionary of job_id -> job_data for liquidity jobs
            is_historical: Whether this is a historical event (for initial loading)
        """
        try:
            # Map the job data to our internal format
            mapped_data = self._map_job_data(job_data)

            job_id = mapped_data.get('job_id')
            user_id = mapped_data.get('user_id')
            job_name = mapped_data.get('name', '').lower()
            event_type = mapped_data.get('event_type')

            if not job_id:
                logger.warning(f"Missing job_id in job data: {job_data}")
                return

            # For updates without user_id, try to find the job in existing state
            if not user_id:
                # Look for this job_id in all users' state
                found = False
                for uid, jobs in jobs_state.items():
                    if job_id in jobs:
                        user_id = uid
                        found = True
                        # Add the user_id to mapped_data for consistency
                        mapped_data['user_id'] = user_id
                        if not is_historical:
                            logger.info(f"Found user_id {user_id} for job {job_id} in existing state")
                        break

                if not found:
                    logger.warning(f"Cannot find user_id for job {job_id} in existing state")
                    return

            if not is_historical:
                logger.info(f"Updating state for job {job_id} for user {user_id}, event: {event_type}")
            elif job_data.get('event_type') == 'Created':
                logger.debug(
                    f"Processing historical Creation event for job {job_id}, user {user_id}, name: '{job_name}'")

            # Initialize user state if needed
            if user_id not in jobs_state:
                jobs_state[user_id] = {}
                if not is_historical:
                    logger.info(f"Created new state for user {user_id}")

            # Update or create job
            if job_id in jobs_state[user_id]:
                # Update existing job
                existing_job = jobs_state[user_id][job_id]

                # Log the current state before updating
                if not is_historical:
                    old_status = existing_job.get('status', 'unknown')
                    old_steps = existing_job.get('completed_steps', 0)
                    old_orders_count = len(existing_job.get('orders', []))
                    logger.info(
                        f"Existing job {job_id} status: {old_status}, steps: {old_steps}, orders: {old_orders_count}")

                updated_fields = []
                if 'status' in mapped_data:
                    existing_job['status'] = mapped_data['status']
                    updated_fields.append(f"status={mapped_data['status']}")
                if 'completed_steps' in mapped_data and mapped_data['completed_steps'] is not None:
                    existing_job['completed_steps'] = mapped_data['completed_steps']
                    updated_fields.append(f"completed_steps={mapped_data['completed_steps']}")
                if 'orders' in mapped_data and mapped_data['orders'] is not None:
                    existing_job['orders'] = mapped_data['orders']
                    updated_fields.append(f"orders={len(mapped_data['orders'])}")

                if not is_historical and updated_fields:
                    logger.info(f"Updated job {job_id} fields: {', '.join(updated_fields)}")

                # Special handling for event-specific fields
                event_type = job_data.get('event_type')
                if event_type == 'StepDone' and 'completed_steps' in job_data:
                    existing_job['completed_steps'] = job_data['completed_steps']
                    if not is_historical:
                        logger.info(f"Updated job {job_id} completed_steps to {job_data['completed_steps']}")
                elif event_type == 'OrdersPlaced' and 'orders' in job_data:
                    existing_job['orders'] = job_data['orders']
                    if not is_historical:
                        logger.info(f"Updated job {job_id} orders, count: {len(job_data['orders'])}")
                elif event_type in ['Paused', 'Resumed', 'Stopped', 'Finished']:
                    existing_job['status'] = event_type
                    if not is_historical:
                        logger.info(f"Updated job {job_id} status to {event_type}")

                # sync changes to the strategy collections
                old_name = existing_job.get('name', '').lower()

                if old_name in ['dca', 'Dca', 'DCA'] and user_id in dca_jobs and job_id in dca_jobs[user_id]:
                    dca_jobs[user_id][job_id] = existing_job
                    if not is_historical:
                        logger.debug(f"Synced job {job_id} updates to DCA collection")
                elif old_name in ['liq', 'Liq', 'LIQ'] and user_id in liq_jobs and job_id in liq_jobs[user_id]:
                    liq_jobs[user_id][job_id] = existing_job
                    if not is_historical:
                        logger.debug(f"Synced job {job_id} updates to LIQ collection")
            else:
                # Create new job
                jobs_state[user_id][job_id] = mapped_data
                if not is_historical:
                    job_status = mapped_data.get('status', 'unknown')
                    logger.info(
                        f"Created new job {job_id} for user {user_id}, name: '{job_name}', status: {job_status}")

                if job_name in ['dca', 'Dca', 'DCA']:
                    if user_id not in dca_jobs:
                        dca_jobs[user_id] = {}
                    dca_jobs[user_id][job_id] = mapped_data
                    if not is_historical:
                        logger.info(f"Added new job {job_id} to DCA jobs collection")
                    else:
                        logger.debug(f"Added historical job {job_id} to DCA jobs collection")
                elif job_name in ['liq', 'Liq', 'LIQ']:
                    if user_id not in liq_jobs:
                        liq_jobs[user_id] = {}
                    liq_jobs[user_id][job_id] = mapped_data
                    if not is_historical:
                        logger.info(f"Added new job {job_id} to LIQ jobs collection")
                    else:
                        logger.debug(f"Added historical job {job_id} to LIQ jobs collection")
                else:
                    if not is_historical:
                        logger.info(f"New job {job_id} with name '{job_name}' not added to any strategy collection")
                    else:
                        logger.debug(
                            f"Historical job {job_id} with name '{job_name}' not added to any strategy collection")

        except Exception as e:
            if not is_historical:
                logger.error(f"Error updating job state: {e}", exc_info=True)
            else:
                logger.debug(f"Error updating historical job state: {e}")

    def analyze_risk(self, job_data: Dict[str, Any], jobs_state: Dict[int, Dict[str, Any]],
                     dca_jobs: Dict[int, Dict[str, Any]], liq_jobs: Dict[int, Dict[str, Any]]) -> Optional[RiskReport]:
        """
        Analyze job data for potential risks

        Args:
            job_data: The job data to analyze
            jobs_state: Current state of all jobs
            dca_jobs: Current state of DCA jobs
            liq_jobs: Current state of liquidity jobs

        Returns:
            RiskReport if risks are detected, None otherwise
        """
        try:
            user_id = job_data.get('user_id')
            job_id = job_data.get('job_id')

            if not user_id:
                logger.warning(f"Skipping risk analysis: Missing user_id for job {job_id}")
                return None

            if not job_id:
                logger.warning(f"Skipping risk analysis: Missing job_id for user {user_id}")
                return None

            # Log start of risk analysis
            logger.debug(f"Starting risk analysis for job {job_id} (user {user_id})")

            # Get the job's completed status
            job = Job(**job_data)

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
                           if getattr(j, 'job_status', getattr(j, 'status', '')) not in ['Finished', 'Stopped']]
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

