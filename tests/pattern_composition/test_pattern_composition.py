"""
Tests for the PatternCompositionEngine.

This ensures that the pattern composition engine correctly identifies composite patterns
from atomic patterns according to the defined rules.
"""
import unittest
from datetime import datetime, timedelta, timezone
from src.risk.pattern_composition import PatternCompositionEngine, CompositePatternRule
from src.models.risk_models import AtomicPattern, CompositePattern, RiskCategory


class TestPatternCompositionEngine(unittest.TestCase):
    def setUp(self):
        self.engine = PatternCompositionEngine()
        self.engine.rules = []  # Clear default rules to ensure test independence

        # Current time for timestamp-based tests
        self.current_time = datetime.now(timezone.utc)

        # Test job IDs
        self.job_id_1 = 101
        self.job_id_2 = 102
        self.job_id_3 = 103

    def test_empty_patterns(self):
        """Test that empty patterns list returns empty result."""
        result = self.engine.process_patterns([])
        self.assertEqual(len(result), 0)

    def test_no_matching_patterns(self):
        """Test that non-matching patterns don't create composite patterns."""
        test_rule = CompositePatternRule(
            rule_id="test_rule",
            pattern_requirements={"test_pattern_a": "1", "test_pattern_b": "1"},
            category=RiskCategory.OVERCONFIDENCE,
            time_window_minutes=60
        )
        self.engine.add_rule(test_rule)
        
        patterns = [
            AtomicPattern(
                pattern_id="test_pattern_a",
                job_id=[self.job_id_1],
                message="Test pattern",
                severity=0.5,
                start_time=self.current_time
            ),
            AtomicPattern(
                pattern_id="unknown_pattern",
                job_id=[self.job_id_1],
                message="Test pattern",
                severity=0.5,
                start_time=self.current_time
            )
        ]

        result = self.engine.process_patterns(patterns)
        self.assertEqual(len(result), 0)

    def test_overtrading_composite_pattern(self):
        """Test detection of overtrading composite pattern."""
        overtrading_rule = CompositePatternRule(
            rule_id="overtrading",
            pattern_requirements={
                "daily_trade_limit": "1",
                "cooldown_limit": "1"
            },
            category=RiskCategory.OVERCONFIDENCE,
            time_window_minutes=60*24,
            sequence_matters=False,
            confidence_boost=0.1,
            message="Overtrading pattern detected for test"
        )
        self.engine.add_rule(overtrading_rule)
        
        patterns = [
            AtomicPattern(
                pattern_id="daily_trade_limit",
                job_id=[self.job_id_1],
                message="Daily trade limit exceeded",
                severity=0.6,
                start_time=self.current_time - timedelta(hours=1)
            ),
            AtomicPattern(
                pattern_id="cooldown_limit",
                job_id=[self.job_id_2],
                message="Cooldown period violated",
                severity=0.7,
                start_time=self.current_time - timedelta(minutes=10)
            )
        ]

        result = self.engine.process_patterns(patterns)

        self.assertEqual(len(result), 1)

        composite = result[0]
        self.assertTrue(composite.pattern_id.startswith("composite_"))
        self.assertTrue("overtrading" in composite.pattern_id)

        expected_base_confidence = (0.6 + 0.7) / 2  # Average of original confidences
        expected_boosted_confidence = min(1.0, expected_base_confidence + 0.1)  # Default boost with cap
        self.assertAlmostEqual(composite.confidence, expected_boosted_confidence, places=5)

        # Test category weights
        self.assertIn(RiskCategory.OVERCONFIDENCE, composite.category_weights)
        self.assertEqual(composite.category_weights[RiskCategory.OVERCONFIDENCE], 0.7)  # Primary category

        # Test composite details with new structure
        self.assertTrue(composite.is_composite)  # Check is_composite flag
        self.assertIn("components", composite.details)  # Check for components list

        # Verify component count
        self.assertEqual(len(composite.details["components"]), 2)

        # Verify component pattern types are present
        component_types = [comp["pattern_type"] for comp in composite.details["components"]]
        self.assertIn("daily_trade_limit", component_types)
        self.assertIn("cooldown_limit", component_types)

        # Check that component IDs are properly formatted
        for component in composite.details["components"]:
            self.assertIn("id", component)
            self.assertIn("pattern_type", component)
            self.assertIn("severity", component)

        # Check that time span info is present
        self.assertIn("time_span", composite.details)
        self.assertIn("duration_minutes", composite.details["time_span"])

    def test_sunk_cost_sequence_matters(self):
        """Test detection of sunk cost pattern where sequence matters."""
        # Create a sequence-dependent rule specifically for this test
        sequence_rule = CompositePatternRule(
            rule_id="loss_escalation",
            pattern_requirements={
                "consecutive_loss": "1",
                "position_size_increase": "1"
            },
            category=RiskCategory.LOSS_BEHAVIOR,
            time_window_minutes=1440,
            sequence_matters=True,
            confidence_boost=0.25,
            message="Loss followed by increasing position size"
        )
        self.engine.add_rule(sequence_rule)
        
        # Loss first, then position increase (correct sequence)
        correct_sequence_patterns = [
            AtomicPattern(
                pattern_id="consecutive_loss",
                job_id=[self.job_id_1],
                message="Consecutive losses detected",
                severity=0.5,
                start_time=self.current_time - timedelta(hours=5)
            ),
            AtomicPattern(
                pattern_id="position_size_increase",
                job_id=[self.job_id_2],
                message="Position size increasing after losses",
                severity=0.6,
                start_time=self.current_time - timedelta(hours=1)
            )
        ]

        # Position increase first, then loss (incorrect sequence)
        incorrect_sequence_patterns = [
            AtomicPattern(
                pattern_id="position_size_increase",
                job_id=[self.job_id_1],
                message="Position size increasing",
                severity=0.6,
                start_time=self.current_time - timedelta(hours=5)
            ),
            AtomicPattern(
                pattern_id="consecutive_loss",
                job_id=[self.job_id_2],
                message="Consecutive losses detected after increase",
                severity=0.5,
                start_time=self.current_time - timedelta(hours=1)
            )
        ]

        # Test correct sequence
        result_correct = self.engine.process_patterns(correct_sequence_patterns)
        self.assertEqual(len(result_correct), 1)
        self.assertTrue("loss_escalation" in result_correct[0].pattern_id)

        # Test incorrect sequence
        result_incorrect = self.engine.process_patterns(incorrect_sequence_patterns)
        self.assertEqual(len(result_incorrect), 0)  # Should not match due to wrong sequence

    def test_position_specific_patterns(self):
        """Test detection of position-specific patterns."""
        # Create a position-specific rule
        position_rule = CompositePatternRule(
            rule_id="position_risk",
            pattern_requirements={
                "high_volatility": "1",
                "low_liquidity": "1"
            },
            category=RiskCategory.OVERCONFIDENCE,
            time_window_minutes=60,
            position_specific=True,
            confidence_boost=0.2,
            message="High risk position detected"
        )
        self.engine.add_rule(position_rule)

        # Create patterns for different positions
        patterns = [
            AtomicPattern(
                pattern_id="high_volatility",
                job_id=[self.job_id_1],
                message="High volatility detected",
                severity=0.7,
                start_time=self.current_time - timedelta(minutes=30),
                positions_key="BYBIT_BTC"
            ),
            AtomicPattern(
                pattern_id="low_liquidity",
                job_id=[self.job_id_1],
                message="Low liquidity detected",
                severity=0.6,
                start_time=self.current_time - timedelta(minutes=20),
                positions_key="BYBIT_BTC"
            ),
            AtomicPattern(
                pattern_id="high_volatility",
                job_id=[self.job_id_2],
                message="High volatility detected",
                severity=0.7,
                start_time=self.current_time - timedelta(minutes=30),
                positions_key="BYBIT_ETH"
            )
        ]

        result = self.engine.process_patterns(patterns)

        # Should have detected one composite pattern for BTC only
        self.assertEqual(len(result), 1)
        composite = result[0]
        self.assertTrue("position_risk" in composite.pattern_id)

        # Verify that only BTC patterns were combined
        component_positions = {comp.get("positions_key") for comp in composite.details["components"]}
        self.assertEqual(len(component_positions), 1)
        self.assertIn("BYBIT_BTC", component_positions)

    def test_optional_patterns(self):
        """Test detection with optional patterns."""
        # Create a rule with optional patterns
        optional_rule = CompositePatternRule(
            rule_id="optional_test",
            pattern_requirements={
                "required_pattern": "1",
                "optional_pattern": "0+"
            },
            category=RiskCategory.OVERCONFIDENCE,
            time_window_minutes=60,
            confidence_boost=0.1,
            message="Pattern with optional components"
        )
        self.engine.add_rule(optional_rule)

        # Test with just required pattern
        patterns_required_only = [
            AtomicPattern(
                pattern_id="required_pattern",
                job_id=[self.job_id_1],
                message="Required pattern",
                severity=0.6,
                start_time=self.current_time - timedelta(minutes=30)
            )
        ]

        # Test with required and optional patterns
        patterns_with_optional = patterns_required_only + [
            AtomicPattern(
                pattern_id="optional_pattern",
                job_id=[self.job_id_2],
                message="Optional pattern 1",
                severity=0.5,
                start_time=self.current_time - timedelta(minutes=20)
            ),
            AtomicPattern(
                pattern_id="optional_pattern",
                job_id=[self.job_id_3],
                message="Optional pattern 2",
                severity=0.4,
                start_time=self.current_time - timedelta(minutes=10)
            )
        ]

        # Both should create composite patterns
        result_required = self.engine.process_patterns(patterns_required_only)
        result_with_optional = self.engine.process_patterns(patterns_with_optional)

        self.assertEqual(len(result_required), 1)
        self.assertEqual(len(result_with_optional), 1)

        # Verify that optional patterns were included when available
        self.assertEqual(len(result_required[0].details["components"]), 1)
        self.assertEqual(len(result_with_optional[0].details["components"]), 3)

    def test_custom_rule(self):
        """Test adding and applying a custom composite pattern rule."""
        # Create a custom rule
        custom_rule = CompositePatternRule(
            rule_id="custom_rule",
            pattern_requirements={
                "pattern_a": "1",
                "pattern_b": "1",
                "pattern_c": "1"
            },
            category=RiskCategory.OVERCONFIDENCE,
            time_window_minutes=30,
            sequence_matters=True,
            confidence_boost=0.15,
            message="Custom pattern detected"
        )
        self.engine.add_rule(custom_rule)

        # Create patterns in correct sequence
        patterns = [
            AtomicPattern(
                pattern_id="pattern_a",
                job_id=[self.job_id_1],
                message="First pattern",
                severity=0.5,
                start_time=self.current_time - timedelta(minutes=20)
            ),
            AtomicPattern(
                pattern_id="pattern_b",
                job_id=[self.job_id_2],
                message="Second pattern",
                severity=0.6,
                start_time=self.current_time - timedelta(minutes=15)
            ),
            AtomicPattern(
                pattern_id="pattern_c",
                job_id=[self.job_id_3],
                message="Third pattern",
                severity=0.7,
                start_time=self.current_time - timedelta(minutes=10)
            )
        ]

        result = self.engine.process_patterns(patterns)
        self.assertEqual(len(result), 1)
        self.assertTrue("custom_rule" in result[0].pattern_id)

    def test_time_window(self):
        """Test that patterns outside the time window are not included."""
        # Create a rule with a short time window
        time_rule = CompositePatternRule(
            rule_id="time_test",
            pattern_requirements={
                "pattern_a": "1",
                "pattern_b": "1"
            },
            category=RiskCategory.OVERCONFIDENCE,
            time_window_minutes=30,
            message="Time window test"
        )
        self.engine.add_rule(time_rule)

        # Create patterns with one outside the time window
        patterns = [
            AtomicPattern(
                pattern_id="pattern_a",
                job_id=[self.job_id_1],
                message="Recent pattern",
                severity=0.5,
                start_time=self.current_time - timedelta(minutes=20)
            ),
            AtomicPattern(
                pattern_id="pattern_b",
                job_id=[self.job_id_2],
                message="Old pattern",
                severity=0.6,
                start_time=self.current_time - timedelta(minutes=40)  # Outside window
            )
        ]

        result = self.engine.process_patterns(patterns)
        self.assertEqual(len(result), 0)  # Should not match due to time window

    def test_pattern_consumption(self):
        """Test that patterns are properly consumed and not reused."""
        # Create a rule that could match multiple times
        consumption_rule = CompositePatternRule(
            rule_id="consumption_test",
            pattern_requirements={
                "pattern_a": "1",
                "pattern_b": "1"
            },
            category=RiskCategory.OVERCONFIDENCE,
            time_window_minutes=60,
            message="Pattern consumption test"
        )
        self.engine.add_rule(consumption_rule)

        # Create patterns that could form multiple matches
        patterns = [
            AtomicPattern(
                pattern_id="pattern_a",
                job_id=[self.job_id_1],
                message="First A",
                severity=0.5,
                start_time=self.current_time - timedelta(minutes=30)
            ),
            AtomicPattern(
                pattern_id="pattern_b",
                job_id=[self.job_id_2],
                message="First B",
                severity=0.6,
                start_time=self.current_time - timedelta(minutes=25)
            ),
            AtomicPattern(
                pattern_id="pattern_a",
                job_id=[self.job_id_3],
                message="Second A",
                severity=0.5,
                start_time=self.current_time - timedelta(minutes=20)
            ),
            AtomicPattern(
                pattern_id="pattern_b",
                job_id=[self.job_id_1],
                message="Second B",
                severity=0.6,
                start_time=self.current_time - timedelta(minutes=15)
            )
        ]

        result = self.engine.process_patterns(patterns)
        self.assertEqual(len(result), 1)  # Should only match once

    def test_pattern_scoring(self):
        """Test the pattern scoring system."""
        # Create a rule with specific scoring
        scoring_rule = CompositePatternRule(
            rule_id="scoring_test",
            pattern_requirements={
                "high_severity": "1",
                "medium_severity": "1",
                "low_severity": "1"
            },
            category=RiskCategory.OVERCONFIDENCE,
            time_window_minutes=60,
            confidence_boost=0.2,
            message="Pattern scoring test"
        )
        self.engine.add_rule(scoring_rule)

        # Create patterns with different severities
        patterns = [
            AtomicPattern(
                pattern_id="high_severity",
                job_id=[self.job_id_1],
                message="High severity pattern",
                severity=0.9,
                start_time=self.current_time - timedelta(minutes=30)
            ),
            AtomicPattern(
                pattern_id="medium_severity",
                job_id=[self.job_id_2],
                message="Medium severity pattern",
                severity=0.5,
                start_time=self.current_time - timedelta(minutes=25)
            ),
            AtomicPattern(
                pattern_id="low_severity",
                job_id=[self.job_id_3],
                message="Low severity pattern",
                severity=0.2,
                start_time=self.current_time - timedelta(minutes=20)
            )
        ]

        result = self.engine.process_patterns(patterns)
        self.assertEqual(len(result), 1)
        
        # Calculate expected confidence
        expected_base_confidence = (0.9 + 0.5 + 0.2) / 3  # Average of severities
        expected_boosted_confidence = min(1.0, expected_base_confidence + 0.2)  # With boost and cap
        
        self.assertAlmostEqual(result[0].confidence, expected_boosted_confidence, places=5)

    def test_multiple_rules(self):
        """Test that multiple rules can be applied to the same patterns."""
        # Create two different rules
        rule1 = CompositePatternRule(
            rule_id="rule1",
            pattern_requirements={
                "pattern_a": "1",
                "pattern_b": "1"
            },
            category=RiskCategory.OVERCONFIDENCE,
            time_window_minutes=60,
            message="First rule"
        )
        
        rule2 = CompositePatternRule(
            rule_id="rule2",
            pattern_requirements={
                "pattern_b": "1",
                "pattern_c": "1"
            },
            category=RiskCategory.OVERCONFIDENCE,
            time_window_minutes=60,
            message="Second rule"
        )
        
        self.engine.add_rule(rule1)
        self.engine.add_rule(rule2)

        # Create patterns that could match both rules
        patterns = [
            AtomicPattern(
                pattern_id="pattern_a",
                job_id=[self.job_id_1],
                message="Pattern A",
                severity=0.5,
                start_time=self.current_time - timedelta(minutes=30)
            ),
            AtomicPattern(
                pattern_id="pattern_b",
                job_id=[self.job_id_2],
                message="Pattern B",
                severity=0.6,
                start_time=self.current_time - timedelta(minutes=25)
            ),
            AtomicPattern(
                pattern_id="pattern_c",
                job_id=[self.job_id_3],
                message="Pattern C",
                severity=0.7,
                start_time=self.current_time - timedelta(minutes=20)
            )
        ]

        result = self.engine.process_patterns(patterns)
        self.assertEqual(len(result), 2)  # Should match both rules

        # Verify both rules were matched
        rule_ids = {comp.pattern_id.split("_")[1] for comp in result}
        self.assertIn("rule1", rule_ids)
        self.assertIn("rule2", rule_ids)


if __name__ == '__main__':
    unittest.main()
