import logging
from datetime import datetime
from typing import Dict, Any, Optional

import requests

from src.models.models import Job, UserLimits, RiskReport
from src.config.config import Config
from src.analyzers.OvertradingAnalyzer import OvertradingAnalyzer

logger = logging.getLogger('trade_guide_health')

class JobProcessor:
    def __init__(self):
        """Initialize the job processor"""
        self.analyzer = OvertradingAnalyzer()
        self.user_limits_cache = {}
        logger.info(f"API base URL: {Config.API_BASE_URL}")

    def _map_job_data(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """Map incoming job data to match our Job model fields"""
        logger.debug(f"Received job data: {job_data}")

        mapped_data = {
            'job_id': job_data.get('job_id'),
            'user_id': job_data.get('user_id'),
            'event_type': job_data.get('event_type'),
            'name': job_data.get('name'),
            'coins': job_data.get('coins', []),
            'side': job_data.get('side'),
            'discount_pct': job_data.get('discount_pct', 0.0),
            'amount': job_data.get('amount', 0.0),
            'steps_total': job_data.get('steps_total', 0),
            'duration_minutes': job_data.get('duration_minutes', 0.0),
            'timestamp': job_data.get('timestamp')
        }

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

    def process_job(self, job_data: Dict[str, Any]) -> Optional[RiskReport]:
        """Process a single job message and return risk report"""
        try:
            mapped_data = self._map_job_data(job_data)

            job = Job(**mapped_data)
            logger.info(f"Processing job {job.id} for user {job.user_id}")

            user_limits = self._fetch_user_limits(job.user_id)
            if not user_limits:
                logger.error(f"Could not fetch limits for user {job.user_id}")
                return None

            # Analyze for overtrading
            risk_report = self.analyzer.analyze(job, user_limits)

            if risk_report.triggers:
                logger.info(f"Risk report generated for user {job.user_id} with {len(risk_report.triggers)} triggers")
            else:
                logger.info(f"No risks detected for user {job.user_id}")

            return risk_report

        except Exception as e:
            logger.error(f"Error processing job: {e}", exc_info=True)
            return None 