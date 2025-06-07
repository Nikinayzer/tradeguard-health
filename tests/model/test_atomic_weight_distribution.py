import unittest
from datetime import datetime, timedelta, timezone
from src.risk.pattern_composition import PatternCompositionEngine, CompositePatternRule
from src.models.risk_models import AtomicPattern, CompositePattern, RiskCategory


class TestAtomicWeightDistribution(unittest.TestCase):
    def setUp(self):
        self.current_time = datetime.now(timezone.utc)

    def test_equal_distribution(self):
        pattern = AtomicPattern(
            pattern_id="position_long_holding_time",
            user_id=1,
            position_key="BYBIT_OP",
            message="TEST",
            description="DESC",
            severity=0.7,
            unique=True,
            ttl_minutes=60 * 24 * 7,
            details={},
            category_weights={
                RiskCategory.OVERCONFIDENCE: 0.2,
            },
        )
        self.assertTrue(len(pattern.category_weights.items()) > 1)
        self.assertEqual(sum(pattern.category_weights.values()), 1.0)
