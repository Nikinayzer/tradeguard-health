"""
Tests for the Position Behavior Evaluator.

These tests verify that the evaluator correctly identifies position management patterns
such as position size acceleration, double down behavior, and asset concentration.
"""
import unittest
from datetime import datetime, timedelta, timezone
from typing import Dict, List

from src.models.job_models import Job
from src.risk.evaluators.trading_behavior import TradingBehaviorEvaluator
from src.models.risk_models import RiskCategory, AtomicPattern
from src.state.state_manager import StateManager


class TestAcceleration(unittest.TestCase):
    def setUp(self):
        """Set up test environment before each test method."""
        self.state_manager = StateManager()
        self.evaluator = TradingBehaviorEvaluator(self.state_manager)
        self.user_id = 12345

        # Use a fixed reference time for consistent testing
        self.reference_time = datetime.now(timezone.utc)

    def _create_test_job(self, job_id: int, coin: str, amount: float,
                         hours_ago: float, status: str = "Created") -> Job:
        """Helper method to create a test job."""
        timestamp = self.reference_time - timedelta(hours=hours_ago)

        return Job(
            job_id=job_id,
            user_id=self.user_id,
            name="DCA",
            coins=[coin],
            side="buy",
            amount=amount,
            discount_pct=0.0,
            steps_total=5,
            duration_minutes=60.0,
            timestamp=timestamp,
            last_updated=timestamp,
            status=status
        )

    def test_no_jobs(self):
        """Test that evaluator handles empty job history."""
        job_history = {}
        patterns = self.evaluator.evaluate(self.user_id, job_history)
        self.assertEqual(len(patterns), 0)

    def test_single_job(self):
        job_history = {
            101: self._create_test_job(101, "BTC", 1000.0, 5.0)
        }
        patterns = self.evaluator.evaluate(self.user_id, job_history)
        self.assertEqual(len(patterns), 0)

    def test_two_jobs_same_coin(self):
        job_history = {
            101: self._create_test_job(101, "BTC", 1000.0, 5.0),
            102: self._create_test_job(102, "BTC", 2000.0, 3.0)
        }

        patterns = self.evaluator.evaluate(self.user_id, job_history)
        self.assertEqual(len(patterns), 0)

    def test_position_size_acceleration(self):
        """Test detection of accelerating position sizes."""
        # Create job history with accelerating position sizes in BTC
        # Job 1: 1000 BTC (5 hours ago)
        # Job 2: 1500 BTC (3 hours ago) - 1.5x increase
        # Job 3: 3000 BTC (1 hour ago) - 2x increase (acceleration)
        job_history = {
            101: self._create_test_job(101, "BTC", 1000.0, 5.0),
            102: self._create_test_job(102, "BTC", 1500.0, 3.0),
            103: self._create_test_job(103, "BTC", 3000.0, 1.0)
        }

        patterns = self.evaluator.evaluate(self.user_id, job_history)

        self.assertEqual(len(patterns), 1)
        pattern = patterns[0]

        self.assertEqual(pattern.pattern_id, "position_acceleration")
        self.assertIn(101, pattern.job_id)
        self.assertIn(102, pattern.job_id)
        self.assertIn(103, pattern.job_id)
        self.assertGreaterEqual(pattern.severity, 0.4)

        # Verify growth ratios (1st growth: 1.5, 2nd growth: 2.0)
        self.assertAlmostEqual(pattern.details["growth_ratios"][0], 1.5, places=2)
        self.assertAlmostEqual(pattern.details["growth_ratios"][1], 2.0, places=2)
        #
        # # Verify acceleration factor (1.33)
        self.assertAlmostEqual(pattern.details["acceleration_factor"], 1.33, places=2)

    def test_position_size_acceleration_multiple_coins(self):
        """Test that acceleration is detected separately for each coin."""
        # Create job history with:
        # - Accelerating positions in BTC
        # - Non-accelerating positions in ETH
        # - No pattern in SOL (only 2 jobs)
        job_history = {
            # BTC jobs (should detect acceleration)
            101: self._create_test_job(101, "BTC", 1000.0, 5.0),
            102: self._create_test_job(102, "BTC", 1500.0, 3.0),
            103: self._create_test_job(103, "BTC", 3000.0, 1.0),

            # ETH jobs (no acceleration - 2nd ratio not higher than 1st)
            201: self._create_test_job(201, "ETH", 500.0, 4.5),
            202: self._create_test_job(202, "ETH", 1000.0, 2.5),  # 2x increase
            203: self._create_test_job(203, "ETH", 1500.0, 0.5),  # 1.5x increase (not accelerating)

            # SOL jobs (only 2 jobs - not enough for acceleration)
            301: self._create_test_job(301, "SOL", 200.0, 4.0),
            302: self._create_test_job(302, "SOL", 400.0, 2.0)
        }

        patterns = self.evaluator.evaluate(self.user_id, job_history)

        # Should only detect one acceleration pattern (BTC)
        self.assertEqual(len(patterns), 1)

        # Verify it's for BTC
        pattern = patterns[0]
        self.assertEqual(pattern.pattern_id, "position_acceleration")
        self.assertEqual(pattern.details["coin"], "BTC")


if __name__ == '__main__':
    unittest.main()
