import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from src.models.models import Job, UserLimits, RiskReport

logger = logging.getLogger('trade_guide_health')

class OvertradingAnalyzer:
    def __init__(self):
        """Initialize the overtrading analyzer"""
        self.user_states: Dict[int, Dict] = {}

    def analyze(self, job: Job, user_limits: UserLimits) -> RiskReport:
        """Analyze a job for overtrading risks"""
        triggers = []
        max_level = 0.0

        # Get or initialize user state
        user_state = self._get_user_state(job.user_id)

        # Check single job limit
        if job.amount > user_limits.max_position_size:
            level = 95.0  # High risk for exceeding single job limit
            max_level = max(max_level, level)
            triggers.append({
                "job_id": job.job_id,
                "message": "Single job limit exceeded",
                "details": {
                    "job_amount": job.amount,
                    "limit": user_limits.max_position_size
                }
            })

        # Check daily trade limit
        if self._check_daily_trade_limit(user_state, user_limits):
            level = 90.0
            max_level = max(max_level, level)
            triggers.append({
                "job_id": job.job_id,
                "message": "Daily trade limit exceeded",
                "details": {
                    "current_trades": len(user_state['trades']),
                    "limit": user_limits.max_daily_trades
                }
            })

        # Check daily volume limit
        if self._check_daily_volume_limit(user_state, user_limits):
            level = 85.0
            max_level = max(max_level, level)
            triggers.append({
                "job_id": job.job_id,
                "message": "Daily volume limit exceeded",
                "details": {
                    "current_volume": sum(t['amount'] for t in user_state['trades']),
                    "limit": user_limits.max_daily_volume
                }
            })

        # Check trade interval
        if self._check_trade_interval(user_state, user_limits):
            level = 75.0
            max_level = max(max_level, level)
            triggers.append({
                "job_id": job.job_id,
                "message": "Trade interval violation",
                "details": {
                    "time_since_last_trade": (datetime.now() - datetime.fromisoformat(user_state['trades'][-1]['timestamp'])).total_seconds() / 60,
                    "min_interval": user_limits.min_trade_interval_minutes
                }
            })

        # Check concurrent jobs
        if self._check_concurrent_jobs(user_state, user_limits):
            level = 80.0
            max_level = max(max_level, level)
            triggers.append({
                "job_id": job.job_id,
                "message": "Too many concurrent jobs",
                "details": {
                    "current_jobs": user_state['active_jobs'],
                    "limit": user_limits.max_concurrent_jobs
                }
            })

        # Update user state
        self._update_user_state(job.user_id, job)

        return RiskReport(
            user_id=job.user_id,
            timestamp=datetime.now().isoformat(),
            type="overtrading",
            level=max_level,
            triggers=triggers
        )

    def _get_user_state(self, user_id: int) -> Dict:
        """Get or initialize user state"""
        if user_id not in self.user_states:
            self.user_states[user_id] = {
                'daily_trades': 0,
                'daily_volume': 0.0,
                'last_trade_time': None,
                'active_jobs': 0,
                'trades': []
            }
        return self.user_states[user_id]

    def _check_daily_trade_limit(self, user_state: Dict, limits: UserLimits) -> bool:
        """Check if daily trade limit is exceeded"""
        today = datetime.now().date()
        user_state['trades'] = [
            trade for trade in user_state['trades']
            if datetime.fromisoformat(trade['timestamp']).date() == today
        ]
        return len(user_state['trades']) >= limits.max_daily_trades

    def _check_daily_volume_limit(self, user_state: Dict, limits: UserLimits) -> bool:
        """Check if daily volume limit is exceeded"""
        today = datetime.now().date()
        daily_volume = sum(
            trade['amount'] for trade in user_state['trades']
            if datetime.fromisoformat(trade['timestamp']).date() == today
        )
        return daily_volume >= limits.max_daily_volume

    def _check_trade_interval(self, user_state: Dict, limits: UserLimits) -> bool:
        """Check if trade interval is violated"""
        if not user_state['trades']:
            return False
        last_trade = datetime.fromisoformat(user_state['trades'][-1]['timestamp'])
        time_since_last_trade = datetime.now() - last_trade
        return time_since_last_trade < timedelta(minutes=limits.min_trade_interval_minutes)

    def _check_concurrent_jobs(self, user_state: Dict, limits: UserLimits) -> bool:
        """Check if concurrent jobs limit is exceeded"""
        return user_state['active_jobs'] >= limits.max_concurrent_jobs

    def _update_user_state(self, user_id: int, job: Job) -> None:
        """Update user state with new job"""
        user_state = self.user_states[user_id]
        user_state['trades'].append({
            'timestamp': job.timestamp,
            'amount': job.amount
        })
        user_state['active_jobs'] += 1 