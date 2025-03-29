import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from src.models.models import Job, UserLimits, RiskReport
from src.utils import log_util

logger = log_util.get_logger()


# TODO MAJOR REFACTORING NEEDED
class OvertradingAnalyzer:
    def __init__(self):
        """Initialize the overtrading analyzer"""
        self.user_states: Dict[int, Dict] = {}

    def analyze(self, job: Job, user_jobs: List[Job], user_limits: UserLimits) -> RiskReport:
        """
        Analyze a job for overtrading risks

        Args:
            job: The current job being processed
            user_jobs: List of active jobs for the user (from our cached state)
            user_limits: The user's trading limits

        Returns:
            RiskReport with any detected triggers
        """
        triggers = []
        max_level = 0.0

        analysis_state = self._build_analysis_state(job.user_id, user_jobs, job)

        active_jobs = [j for j in user_jobs if
                       getattr(j, 'job_status', getattr(j, 'status', '')) not in ['Finished', 'Stopped']]
        logger.info(
            f"Analyzing risks for job {job.job_id} (user {job.user_id}): {len(active_jobs)} active jobs, {len(analysis_state['trades'])} recent trades")

        """Single job limit"""
        if job.amount > user_limits.max_position_size:
            level = 95.0  # todo dynamic evaluation; also dca handling since it's relative, not absolute
            max_level = max(max_level, level)
            logger.warning(f"Job {job.job_id} exceeds single job limit: {job.amount} > {user_limits.max_position_size}")
            triggers.append({
                "job_id": job.job_id,
                "message": "Single job limit exceeded",
                "details": {
                    "job_amount": job.amount,
                    "limit": user_limits.max_position_size
                }
            })

        """Daily trades count"""
        if len(analysis_state['trades']) >= user_limits.max_daily_trades:
            level = 90.0
            max_level = max(max_level, level)
            logger.warning(
                f"User {job.user_id} exceeds daily trade limit: {len(analysis_state['trades'])} >= {user_limits.max_daily_trades}")
            triggers.append({
                "job_id": job.job_id,
                "message": "Daily trade limit exceeded",
                "details": {
                    "current_trades": len(analysis_state['trades']),
                    "limit": user_limits.max_daily_trades
                }
            })

        """Daily Volume"""
        daily_volume = sum(t['amount'] for t in analysis_state['trades'])
        if daily_volume >= user_limits.max_daily_volume:
            level = 85.0
            max_level = max(max_level, level)
            logger.warning(
                f"User {job.user_id} exceeds daily volume limit: {daily_volume} >= {user_limits.max_daily_volume}")
            triggers.append({
                "job_id": job.job_id,
                "message": "Daily volume limit exceeded",
                "details": {
                    "current_volume": daily_volume,
                    "limit": user_limits.max_daily_volume
                }
            })

        """Cooldown between jobs"""
        if job.event_type == 'Created':
            logger.debug(
                f"Checking trading cooldown for job {job.job_id} (min interval: {user_limits.min_trade_interval_minutes} minutes)")
            interval_check = self._check_trade_interval(analysis_state, user_limits, job)

            if interval_check['minutes_since_last'] is not None:
                logger.debug(
                    f"Time since last trade: {interval_check['minutes_since_last']:.2f} minutes, minimum required: {interval_check['min_interval']} minutes")

            if interval_check['violated']:
                level = 75.0
                max_level = max(max_level, level)

                # Calculate when CD ends
                cooldown_ends_in = interval_check['min_interval'] - interval_check['minutes_since_last']
                cooldown_ends_at = datetime.now() + timedelta(minutes=cooldown_ends_in)

                logger.warning(
                    f"Trading cooldown violation! User {job.user_id} must wait {cooldown_ends_in:.2f} more minutes (until {cooldown_ends_at.strftime('%H:%M:%S')})")

                triggers.append({
                    "job_id": job.job_id,
                    "message": "Trading cooldown period violation",
                    "details": {
                        "time_since_last_trade": interval_check['minutes_since_last'],
                        "min_interval": interval_check['min_interval'],
                        "cooldown_ends_in": cooldown_ends_in,
                        "cooldown_ends_at": cooldown_ends_at.isoformat()
                    }
                })
                logger.warning(f"Adding cooldown violation trigger for job {job.job_id}")

        """Concurrent jobs"""
        active_job_count = len(active_jobs)
        if active_job_count >= user_limits.max_concurrent_jobs:
            level = 80.0
            max_level = max(max_level, level)
            logger.warning(
                f"User {job.user_id} exceeds max concurrent jobs: {active_job_count} >= {user_limits.max_concurrent_jobs}")

            active_job_ids = [j.job_id for j in active_jobs]
            logger.debug(f"Active jobs for user {job.user_id}: {active_job_ids}")

            triggers.append({
                "job_id": job.job_id,
                "message": "Too many concurrent jobs",
                "details": {
                    "current_jobs": active_job_count,
                    "limit": user_limits.max_concurrent_jobs,
                    "active_job_ids": active_job_ids
                }
            })

        risk_report = RiskReport(
            user_id=job.user_id,
            timestamp=datetime.now().isoformat(),
            type="overtrading",
            level=max_level,
            triggers=triggers
        )

        if triggers:
            logger.info(f"Found {len(triggers)} risk triggers for job {job.job_id} with risk level {max_level}")
            for i, trigger in enumerate(triggers):
                logger.info(f"Trigger {i + 1}: {trigger['message']} - {trigger['details']}")
        else:
            logger.info(f"No risk triggers found for job {job.job_id}")

        return risk_report

    def _build_analysis_state(self, user_id: int, user_jobs: List[Job], current_job: Job = None) -> Dict:
        """
        Build analysis state from list of user jobs

        Args:
            user_id: The user ID
            user_jobs: List of active jobs for the user
            current_job: The current job being analyzed (may not be in user_jobs yet)

        Returns:
            Dictionary with analysis state
        """
        today = datetime.now(timezone.utc).date()
        now = datetime.now(timezone.utc)
        yesterday = now - timedelta(days=1)

        logger.info(
            f"Building analysis state for user {user_id} for trades since {yesterday.strftime('%Y-%m-%d %H:%M:%S')}")

        analysis_state = {
            'trades': []
        }

        # Count of existing jobs plus the current one being analyzed
        job_count = len(user_jobs) + (
            1 if current_job is not None and current_job.job_id not in [j.job_id for j in user_jobs] else 0)
        logger.info(f"Processing {job_count} jobs for user {user_id} (including current job if new)")

        existing_job_count = 0
        today_job_count = 0

        for job in user_jobs:
            is_today = self._add_job_to_analysis(job, analysis_state, today)
            existing_job_count += 1
            if is_today:
                today_job_count += 1

        logger.info(f"Processed {existing_job_count} existing jobs, {today_job_count} from today")

        # Add the current job if it's not already in the list
        if current_job is not None and current_job.job_id not in [j.job_id for j in user_jobs]:
            is_today = self._add_job_to_analysis(current_job, analysis_state, today)
            logger.info(f"Added current job {current_job.job_id} to analysis state, is from today: {is_today}")

        # Sort trades by timestamp
        if analysis_state['trades']:
            # Sort by parsed_time, which is a datetime object
            analysis_state['trades'].sort(key=lambda x: x['parsed_time'])

            # Log the sorted trades for debugging
            trade_count = len(analysis_state['trades'])
            logger.info(f"Collected {trade_count} trades from today for user {user_id}")

            if trade_count > 0:
                first_trade = analysis_state['trades'][0]
                last_trade = analysis_state['trades'][-1]

                # Calculate time range
                time_span_minutes = (last_trade['parsed_time'] - first_trade[
                    'parsed_time']).total_seconds() / 60 if trade_count > 1 else 0

                logger.info(f"Trade time range: {time_span_minutes:.2f} minutes")
                logger.info(
                    f"First trade: Job {first_trade['job_id']} ({first_trade['name']}) at {first_trade['timestamp']}")
                logger.info(
                    f"Last trade: Job {last_trade['job_id']} ({last_trade['name']}) at {last_trade['timestamp']}")

                trade_names = [f"{t['job_id']}:{t['name']}" for t in analysis_state['trades']]
                logger.debug(f"Today's trades: {trade_names}")
        else:
            logger.info(f"No trades from today found for user {user_id}")

        return analysis_state

    def _add_job_to_analysis(self, job, analysis_state, today):
        """
        Helper method to add a job to the analysis state

        Args:
            job: The job to add
            analysis_state: The analysis state dictionary
            today: Today's date for filtering

        Returns:
            bool: True if job was added to today's trades, False otherwise
        """
        try:
            # Parse timestamp consistently to avoid timezone issues
            timestamp = job.timestamp

            # Skip if no timestamp
            if not timestamp:
                logger.debug(f"Skipping job {job.job_id} - no timestamp")
                return False

            # Remove timezone info to make naive datetime
            clean_timestamp = timestamp
            if 'Z' in timestamp:
                # UTC timezone with Z
                clean_timestamp = timestamp.replace('Z', '')
            elif '+' in timestamp:
                # Positive timezone offset
                clean_timestamp = timestamp.split('+')[0]
            elif '-' in timestamp and 'T' in timestamp:
                # Check if this is a negative timezone offset or just a date separator
                last_dash = timestamp.rindex('-')
                if last_dash > 10:  # Position after YYYY-MM-DD
                    clean_timestamp = timestamp[:last_dash]

            # Parse
            job_datetime = datetime.fromisoformat(clean_timestamp).replace(tzinfo=timezone.utc)
            job_date = job_datetime.date()
            logger.debug(f"Job {job.job_id} timestamp: {clean_timestamp}, date: {job_date}, today: {today}")

            if job_date == today:
                trade_info = {
                    'job_id': job.job_id,
                    'timestamp': clean_timestamp,
                    'parsed_time': job_datetime,
                    'amount': job.amount,
                    'status': getattr(job, 'status', getattr(job, 'job_status', 'unknown')),
                    'name': getattr(job, 'name', 'unknown')
                }
                analysis_state['trades'].append(trade_info)
                logger.debug(f"Added job {job.job_id} to analysis state, timestamp: {clean_timestamp}")
                return True
            else:
                logger.debug(f"Skipping job {job.job_id} - not from today (date: {job_date})")
                return False
        except (ValueError, TypeError) as e:
            logger.warning(f"Invalid timestamp in job: {job.job_id}, {e}")
            return False

    def _check_trade_interval(self, analysis_state: Dict, limits: UserLimits, current_job: Job = None) -> Dict:
        """
        Check if trade interval is violated

        Returns dictionary with:
            - violated: bool - whether the interval is violated
            - minutes_since_last: float - minutes since last trade
            - min_interval: int - minimum required interval in minutes
        """
        result = {
            'violated': False,
            'minutes_since_last': None,
            'min_interval': limits.min_trade_interval_minutes
        }

        # Need at least one previous trade to check interval
        if not analysis_state['trades']:
            logger.info(
                f"No previous trades found for user {current_job.user_id if current_job else 'unknown'} - no cooldown check needed")
            return result

        try:
            # Parse current job time, defaulting to "now" if not provided
            if current_job and current_job.timestamp:
                # Parse current job timestamp
                current_timestamp = current_job.timestamp

                # Remove timezone info to make naive datetime
                if 'Z' in current_timestamp:
                    current_timestamp = current_timestamp.replace('Z', '')
                elif '+' in current_timestamp:
                    current_timestamp = current_timestamp.split('+')[0]
                elif '-' in current_timestamp and 'T' in current_timestamp:
                    last_dash = current_timestamp.rindex('-')
                    if last_dash > 10:  # Position after YYYY-MM-DD
                        current_timestamp = current_timestamp[:last_dash]

                try:
                    current_time = datetime.fromisoformat(current_timestamp)
                    current_time = current_time.replace(tzinfo=timezone.utc)
                    logger.info(f"Using job timestamp for cooldown check: {current_timestamp}")
                except ValueError:
                    # Fallback to now if parsing fails
                    logger.warning(
                        f"Could not parse current job timestamp: {current_timestamp}, falling back to current time")
                    current_time = datetime.now(timezone.utc)
            else:
                # Use current time if no job provided
                current_time = datetime.now(timezone.utc)
                logger.info("Using current time for cooldown check")

            previous_trades = [t for t in analysis_state['trades']
                               if current_job is None or t['job_id'] != current_job.job_id]

            if not previous_trades:
                logger.info(
                    f"No previous trades found for user {current_job.user_id if current_job else 'unknown'} (excluding current job) - no cooldown check needed")
                return result

            previous_trades.sort(key=lambda x: x['parsed_time'], reverse=True)
            last_trade_data = previous_trades[0]
            last_trade_time = last_trade_data['parsed_time']

            logger.debug(
                f"Last trade was job {last_trade_data['job_id']} at {last_trade_data['timestamp']} ({last_trade_data['name']})")

            # Calculate time difference
            time_delta = current_time - last_trade_time
            minutes_since_last = time_delta.total_seconds() / 60

            result['minutes_since_last'] = minutes_since_last
            result['violated'] = minutes_since_last < limits.min_trade_interval_minutes

            # Calculate when cooldown ends (for logging purposes)
            if result['violated']:
                cooldown_ends_in = limits.min_trade_interval_minutes - minutes_since_last
                cooldown_ends_at = datetime.now() + timedelta(minutes=cooldown_ends_in)
                logger.warning(
                    f"Cooldown violation detected! Only {minutes_since_last:.2f} minutes since last trade (job {last_trade_data['job_id']})")
                logger.debug(
                    f"User must wait {cooldown_ends_in:.2f} more minutes until {cooldown_ends_at.strftime('%H:%M:%S')} before trading")
            else:
                logger.info(
                    f"Cooldown check passed: {minutes_since_last:.2f} minutes since last trade (job {last_trade_data['job_id']}), minimum required: {limits.min_trade_interval_minutes}")

            return result
        except Exception as e:
            logger.exception(f"Error checking trade interval: {e}")
            return result
