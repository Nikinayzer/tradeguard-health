"""
Tests for the PatternCompositionEngine.

This ensures that the pattern composition engine correctly identifies composite patterns
from atomic patterns according to the defined rules.
"""
import unittest
from datetime import datetime, timedelta
from src.risk.pattern_composition import PatternCompositionEngine, CompositePatternRule
from src.models.risk_models import Pattern, RiskCategory


class TestPatternCompositionEngine(unittest.TestCase):
    def setUp(self):
        # Create a pattern composition engine with default rules
        self.engine = PatternCompositionEngine()

        # Current time for timestamp-based tests
        self.current_time = datetime.now()

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
        patterns = [
            Pattern(
                pattern_id="unknown_pattern",
                job_id=[self.job_id_1],
                message="Test pattern",
                confidence=0.5,
                category_weights={RiskCategory.FOMO: 1.0},
                start_time=self.current_time
            )
        ]

        result = self.engine.process_patterns(patterns)
        self.assertEqual(len(result), 0)

    def test_overtrading_composite_pattern(self):
        """Test detection of rapid overtrading composite pattern."""
        patterns = [
            Pattern(
                pattern_id="daily_trade_limit",
                job_id=[self.job_id_1],
                message="Daily trade limit exceeded",
                confidence=0.6,
                category_weights={RiskCategory.OVERTRADING: 0.8, RiskCategory.FOMO: 0.2},
                start_time=self.current_time - timedelta(hours=1)
            ),
            Pattern(
                pattern_id="cooldown_limit",
                job_id=[self.job_id_2],
                message="Cooldown period violated",
                confidence=0.7,
                category_weights={RiskCategory.OVERTRADING: 0.6, RiskCategory.FOMO: 0.4},
                start_time=self.current_time - timedelta(minutes=10)
            )
        ]

        result = self.engine.process_patterns(patterns, self.current_time)

        # Should have detected one composite pattern
        self.assertEqual(len(result), 1)

        # Verify the composite pattern properties
        composite = result[0]
        self.assertTrue(composite.pattern_id.startswith("composite_"))
        self.assertTrue("rapid_overtrading" in composite.pattern_id)

        # Test confidence boosting
        expected_base_confidence = (0.6 + 0.7) / 2  # Average of original confidences
        expected_min_boosted_confidence = expected_base_confidence + 0.1  # Default boost
        self.assertGreaterEqual(composite.confidence, expected_min_boosted_confidence)

        # Test category weights
        self.assertIn(RiskCategory.OVERTRADING, composite.category_weights)
        self.assertEqual(composite.category_weights[RiskCategory.OVERTRADING], 0.7)  # Primary category

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
            # Check ID format - should be like "pattern_type:hash"
            self.assertTrue(":" in component["id"])
            
        # Check that time span info is present
        self.assertIn("time_span", composite.details)

    def test_sunk_cost_sequence_matters(self):
        """Test detection of sunk cost pattern where sequence matters."""
        # Loss first, then position increase (correct sequence)
        correct_sequence_patterns = [
            Pattern(
                pattern_id="consecutive_loss",
                job_id=[self.job_id_1],
                message="Consecutive losses detected",
                confidence=0.5,
                category_weights={RiskCategory.SUNK_COST: 1.0},
                start_time=self.current_time - timedelta(hours=5)
            ),
            Pattern(
                pattern_id="position_size_increase",
                job_id=[self.job_id_2],
                message="Position size increasing after losses",
                confidence=0.6,
                category_weights={RiskCategory.SUNK_COST: 0.7, RiskCategory.FOMO: 0.3},
                start_time=self.current_time - timedelta(hours=1)
            )
        ]

        # Position increase first, then loss (incorrect sequence)
        incorrect_sequence_patterns = [
            Pattern(
                pattern_id="position_size_increase",
                job_id=[self.job_id_1],
                message="Position size increasing",
                confidence=0.6,
                category_weights={RiskCategory.SUNK_COST: 0.7, RiskCategory.FOMO: 0.3},
                start_time=self.current_time - timedelta(hours=5)
            ),
            Pattern(
                pattern_id="consecutive_loss",
                job_id=[self.job_id_2],
                message="Consecutive losses detected after increase",
                confidence=0.5,
                category_weights={RiskCategory.SUNK_COST: 1.0},
                start_time=self.current_time - timedelta(hours=1)
            )
        ]

        # Test correct sequence
        result_correct = self.engine.process_patterns(correct_sequence_patterns, self.current_time)
        self.assertEqual(len(result_correct), 1)
        self.assertTrue("loss_escalation" in result_correct[0].pattern_id)

        # Test incorrect sequence
        result_incorrect = self.engine.process_patterns(incorrect_sequence_patterns, self.current_time)
        self.assertEqual(len(result_incorrect), 0)  # Should not match due to wrong sequence

    def test_relative_time_window(self):
        """Test that patterns close to each other in time but old relative to current time still match."""
        # Create patterns from yesterday but close to each other in time
        old_patterns = [
            Pattern(
                pattern_id="daily_trade_limit",
                job_id=[self.job_id_1],
                message="Daily trade limit from yesterday",
                confidence=0.6,
                category_weights={RiskCategory.OVERTRADING: 1.0},
                start_time=self.current_time - timedelta(hours=25)  # Yesterday
            ),
            Pattern(
                pattern_id="cooldown_limit",
                job_id=[self.job_id_2],
                message="Cooldown violation from yesterday",
                confidence=0.7,
                category_weights={RiskCategory.OVERTRADING: 1.0},
                start_time=self.current_time - timedelta(hours=24.5)  # 30 min later
            )
        ]

        # They should match because they're close to each other, even though old
        result = self.engine.process_patterns(old_patterns, self.current_time)
        self.assertEqual(len(result), 1)
        self.assertTrue("rapid_overtrading" in result[0].pattern_id)

    def test_patterns_too_far_apart(self):
        """Test that patterns too far apart in time don't match."""
        # Create patterns that are both recent but too far apart (rapid_overtrading, 11 hours time window)
        spread_patterns = [
            Pattern(
                pattern_id="daily_trade_limit",
                job_id=[self.job_id_1],
                message="Trade limit from earlier today",
                confidence=0.6,
                category_weights={RiskCategory.OVERTRADING: 1.0},
                start_time=self.current_time - timedelta(hours=14)  # 14 hours ago
            ),
            Pattern(
                pattern_id="cooldown_limit",
                job_id=[self.job_id_2],
                message="Recent cooldown violation",
                confidence=0.7,
                category_weights={RiskCategory.OVERTRADING: 1.0},
                start_time=self.current_time - timedelta(hours=1)  # 1 hour ago
            )
        ]

        # They shouldn't match because they're more than 6 hours apart (rapid_overtrading rule)
        result = self.engine.process_patterns(spread_patterns, self.current_time)
        self.assertEqual(len(result), 0)

    def test_custom_rule(self):
        """Test adding and applying a custom composite pattern rule."""
        # Create a custom rule
        custom_rule = CompositePatternRule(
            rule_id="custom_fomo_test",
            pattern_ids=["market_volatility", "rapid_trade"],
            category=RiskCategory.FOMO,
            time_window_minutes=60,  # 1 hour window
            confidence_boost=0.3,
            message="Custom FOMO pattern detected"
        )

        # Create a new engine and add the custom rule
        custom_engine = PatternCompositionEngine()
        custom_engine.add_rule(custom_rule)

        # Create patterns matching the custom rule
        patterns = [
            Pattern(
                pattern_id="market_volatility",
                job_id=[self.job_id_1],
                message="High market volatility",
                confidence=0.4,
                category_weights={RiskCategory.FOMO: 0.8, RiskCategory.SUNK_COST: 0.2},
                start_time=self.current_time - timedelta(minutes=30)
            ),
            Pattern(
                pattern_id="rapid_trade",
                job_id=[self.job_id_2],
                message="Rapid trading detected",
                confidence=0.5,
                category_weights={RiskCategory.FOMO: 0.7, RiskCategory.OVERTRADING: 0.3},
                start_time=self.current_time - timedelta(minutes=10)
            )
        ]

        result = custom_engine.process_patterns(patterns, self.current_time)

        # Should have detected one composite pattern
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].pattern_id, "composite_custom_fomo_test")

        # Test confidence boosting for custom rule
        expected_base_confidence = (0.4 + 0.5) / 2  # Average of original confidences
        expected_boosted_confidence = expected_base_confidence + 0.3  # Custom boost
        self.assertAlmostEqual(result[0].confidence, expected_boosted_confidence, places=5)

    def test_pattern_with_duration(self):
        """Test detection with patterns that have both start and end times."""
        patterns = [
            Pattern(
                pattern_id="daily_trade_limit",
                job_id=[self.job_id_1],
                message="Trading limit exceeded over time period",
                confidence=0.6,
                category_weights={RiskCategory.OVERTRADING: 1.0},
                start_time=self.current_time - timedelta(hours=3),
                end_time=self.current_time - timedelta(hours=2)  # 1 hour duration
            ),
            Pattern(
                pattern_id="cooldown_limit",
                job_id=[self.job_id_2],
                message="Cooldown violation",
                confidence=0.7,
                category_weights={RiskCategory.OVERTRADING: 1.0},
                start_time=self.current_time - timedelta(hours=1.5)  # Starts after first pattern ends
            )
        ]

        result = self.engine.process_patterns(patterns, self.current_time)
        self.assertEqual(len(result), 1)
        self.assertIn("rapid_overtrading", result[0].pattern_id)

    def test_multiple_job_ids_combined(self):
        """Test that job IDs from source patterns are combined in composite pattern."""
        patterns = [
            Pattern(
                pattern_id="daily_trade_limit",
                job_id=[self.job_id_1, self.job_id_2],
                message="Daily trade limit exceeded for multiple jobs",
                confidence=0.6,
                category_weights={RiskCategory.OVERTRADING: 1.0},
                start_time=self.current_time - timedelta(hours=1)
            ),
            Pattern(
                pattern_id="cooldown_limit",
                job_id=[self.job_id_2, self.job_id_3],
                message="Cooldown period violated",
                confidence=0.7,
                category_weights={RiskCategory.OVERTRADING: 1.0},
                start_time=self.current_time - timedelta(minutes=30)
            )
        ]

        result = self.engine.process_patterns(patterns, self.current_time)

        # Should have combined all job IDs
        self.assertEqual(len(result), 1)
        self.assertEqual(len(result[0].job_id), 3)
        self.assertIn(self.job_id_1, result[0].job_id)
        self.assertIn(self.job_id_2, result[0].job_id)
        self.assertIn(self.job_id_3, result[0].job_id)

    def test_overlapping_patterns(self):
        """Test detection of patterns that overlap in time."""
        # Create patterns that overlap in time
        patterns = [
            Pattern(
                pattern_id="daily_trade_limit",
                job_id=[self.job_id_1],
                message="Trading limit exceeded with duration",
                confidence=0.6,
                category_weights={RiskCategory.OVERTRADING: 1.0},
                start_time=self.current_time - timedelta(hours=3),
                end_time=self.current_time - timedelta(hours=1)  # 2-hour duration
            ),
            Pattern(
                pattern_id="cooldown_limit",
                job_id=[self.job_id_2],
                message="Cooldown violation during active trading",
                confidence=0.7,
                category_weights={RiskCategory.OVERTRADING: 1.0},
                start_time=self.current_time - timedelta(hours=2),  # Starts during first pattern
                end_time=self.current_time - timedelta(minutes=30)  # Ends after first pattern
            )
        ]

        result = self.engine.process_patterns(patterns, self.current_time)
        
        # Should detect the composite pattern despite overlap
        self.assertEqual(len(result), 1)
        self.assertIn("rapid_overtrading", result[0].pattern_id)
        
        # Verify the composite pattern has correct time boundaries
        self.assertEqual(result[0].start_time, patterns[0].start_time)
        self.assertEqual(result[0].end_time, patterns[1].end_time)
        
        # Check duration in the new structure
        self.assertIn("time_span", result[0].details)
        self.assertIn("duration_minutes", result[0].details["time_span"])
        
        # Calculate expected duration
        expected_duration = (result[0].end_time - result[0].start_time).total_seconds() / 60
        actual_duration = result[0].details["time_span"]["duration_minutes"]
        
        # Verify duration is correct
        self.assertAlmostEqual(actual_duration, expected_duration, places=1)

    def test_require_all_patterns(self):
        """Test that require_all_patterns flag correctly enforces all patterns to be present."""
        # Create a custom engine with two rules - one requiring all patterns, one not
        custom_engine = PatternCompositionEngine()
        
        # Rule requiring all patterns (3 specific patterns)
        all_required_rule = CompositePatternRule(
            rule_id="all_required_test",
            pattern_ids=["pattern_a", "pattern_b", "pattern_c"],
            category=RiskCategory.FOMO,
            time_window_minutes=60,
            sequence_matters=False,
            require_all_patterns=True,  # Require ALL patterns to be present
            confidence_boost=0.2,
            message="All three patterns detected"
        )
        
        # Rule requiring only 2 out of 3 patterns
        some_required_rule = CompositePatternRule(
            rule_id="some_required_test",
            pattern_ids=["pattern_a", "pattern_b", "pattern_c"],
            category=RiskCategory.FOMO,
            time_window_minutes=60,
            sequence_matters=False,
            min_patterns_required=2,  # Only 2 out of 3 required
            require_all_patterns=False,
            confidence_boost=0.1,
            message="At least two patterns detected"
        )
        
        custom_engine.add_rule(all_required_rule)
        custom_engine.add_rule(some_required_rule)
        
        # Create patterns - only two out of three types
        incomplete_patterns = [
            Pattern(
                pattern_id="pattern_a",
                job_id=[self.job_id_1],
                message="Pattern A detected",
                confidence=0.5,
                category_weights={RiskCategory.FOMO: 1.0},
                start_time=self.current_time - timedelta(minutes=30)
            ),
            Pattern(
                pattern_id="pattern_b",
                job_id=[self.job_id_2],
                message="Pattern B detected",
                confidence=0.6,
                category_weights={RiskCategory.FOMO: 1.0},
                start_time=self.current_time - timedelta(minutes=15)
            )
        ]
        
        # Test with incomplete patterns - should only match "some_required_rule"
        result_incomplete = custom_engine.process_patterns(incomplete_patterns, self.current_time)
        self.assertEqual(len(result_incomplete), 1)
        self.assertEqual(result_incomplete[0].pattern_id, "composite_some_required_test")
        
        # Add the third pattern type
        complete_patterns = incomplete_patterns + [
            Pattern(
                pattern_id="pattern_c",
                job_id=[self.job_id_3],
                message="Pattern C detected",
                confidence=0.7,
                category_weights={RiskCategory.FOMO: 1.0},
                start_time=self.current_time - timedelta(minutes=5)
            )
        ]
        
        # Test with complete patterns - should match both rules
        result_complete = custom_engine.process_patterns(complete_patterns, self.current_time)
        self.assertEqual(len(result_complete), 2)
        
        # Check that both rules matched - order may vary
        rule_ids = {result.pattern_id for result in result_complete}
        self.assertIn("composite_all_required_test", rule_ids)
        self.assertIn("composite_some_required_test", rule_ids)
        
        # Check that the confidence boost was applied correctly
        for result in result_complete:
            if result.pattern_id == "composite_all_required_test":
                # Average of base confidences (0.5 + 0.6 + 0.7)/3 = 0.6 plus 0.2 boost
                self.assertAlmostEqual(result.confidence, 0.8, places=5)
            elif result.pattern_id == "composite_some_required_test":
                # For some_required, could match different subsets, but the confidence
                # should reflect the proper boost regardless
                self.assertGreaterEqual(result.confidence, 0.1)  # At least the boost amount


if __name__ == '__main__':
    unittest.main()
