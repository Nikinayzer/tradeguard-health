import unittest
from datetime import datetime, timedelta, timezone
from src.models.risk_models import AtomicPattern, RiskCategory
from src.risk.pattern_composition import PatternCompositionEngine
from src.state.state_manager import StateManager


class TestRiskSeekingComposite(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.state_manager = StateManager()
        self.pattern_storage = self.state_manager.pattern_storage
        self.composition_engine = PatternCompositionEngine()
        self.user_id = 1

    def tearDown(self):
        """Clean up test fixtures after each test method."""
        self.pattern_storage.clear_all_patterns()

    def test_risk_seeking_composite_pattern(self):
        """Test that risk-seeking composite pattern is detected when both long holding time and unrealized PnL
        patterns exist."""
        current_time = datetime.now(timezone.utc)

        long_holding_pattern = AtomicPattern(
            pattern_id="position_long_holding_time",
            message="Position held for extended period",
            severity=0.7,
            start_time=current_time - timedelta(days=5),
            end_time=current_time,
            positions_key="BYBIT_BTC",
            unique=True
        )

        unrealized_pnl_pattern = AtomicPattern(
            pattern_id="position_unrealized_pnl_threshold",
            message="Significant unrealized PnL detected",
            severity=0.8,
            start_time=current_time - timedelta(days=2),
            end_time=current_time,
            positions_key="BYBIT_BTC",
            unique=True
        )

        self.pattern_storage.store_patterns(self.user_id, [long_holding_pattern, unrealized_pnl_pattern])
        stored_patterns = self.pattern_storage.get_user_patterns(self.user_id)
        self.assertEqual(len(stored_patterns), 2, "Should have stored 2 patterns")

        composite_patterns = self.composition_engine.process_patterns(stored_patterns)
        self.assertEqual(len(composite_patterns), 1, "Should have created 1 composite pattern")

        composite = composite_patterns[0]
        self.assertEqual(composite.pattern_id, "composite_risk_seeking", "Should be risk-seeking composite pattern")
        self.assertEqual(composite.category_weights[RiskCategory.LOSS_BEHAVIOR], 0.7,
                         "Should have correct category weight")

        self.assertEqual(len(composite.component_patterns), 2, "Should have 2 component patterns")

        expected_confidence = (0.7 + 0.8) / 2  # Average of component severities
        # self.assertAlmostEqual(composite.confidence, expected_confidence, places=2, msg="Composite confidence should be average of component severities")

        self.assertEqual(composite.start_time, long_holding_pattern.start_time,
                         "Composite start time should match earliest pattern")
        self.assertEqual(composite.end_time, current_time,
                         "Composite end time should match latest pattern")

    def test_risk_seeking_composite_outside_time_window(self):
        """Test that risk-seeking composite pattern is not detected when patterns are outside time window."""
        current_time = datetime.now(timezone.utc)

        long_holding_pattern = AtomicPattern(
            pattern_id="position_long_holding_time",
            message="Position held for extended period",
            severity=0.7,
            start_time=current_time - timedelta(days=8),
            positions_key="BYBIT_BTC",
            unique=True
        )

        unrealized_pnl_pattern = AtomicPattern(
            pattern_id="position_unrealized_pnl_threshold",
            message="Significant unrealized PnL detected",
            severity=0.8,
            start_time=current_time - timedelta(days=2),
            end_time=current_time,
            positions_key="BYBIT_BTC",
            unique=True
        )

        self.pattern_storage.store_patterns(self.user_id, [long_holding_pattern, unrealized_pnl_pattern])
        stored_patterns = self.pattern_storage.get_user_patterns(self.user_id)
        composite_patterns = self.composition_engine.process_patterns(stored_patterns)

        self.assertEqual(len(composite_patterns), 0,
                         "Should not create composite pattern when patterns are outside time window")

    def test_risk_seeking_composite_different_positions(self):
        """Test that risk-seeking composite pattern is not detected when patterns are for different positions."""
        current_time = datetime.now(timezone.utc)

        long_holding_pattern = AtomicPattern(
            pattern_id="position_long_holding_time",
            message="Position held for extended period",
            severity=0.7,
            start_time=current_time - timedelta(days=5),
            positions_key="BYBIT_BTC",
            unique=True
        )

        unrealized_pnl_pattern = AtomicPattern(
            pattern_id="position_unrealized_pnl_threshold",
            message="Significant unrealized PnL detected",
            severity=0.8,
            start_time=current_time - timedelta(days=2),
            positions_key="BYBIT_ETH",
            unique=True
        )

        self.pattern_storage.store_patterns(self.user_id, [long_holding_pattern, unrealized_pnl_pattern])
        stored_patterns = self.pattern_storage.get_user_patterns(self.user_id)

        composite_patterns = self.composition_engine.process_patterns(stored_patterns)
        self.assertEqual(len(composite_patterns), 0,
                         "Should not create composite pattern when patterns are for different positions")


if __name__ == '__main__':
    unittest.main()
