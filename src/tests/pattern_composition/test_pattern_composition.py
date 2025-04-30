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
        self.current_time = datetime.now()

        # Test job IDs
        self.job_id_1 = 101
        self.job_id_2 = 102
        self.job_id_3 = 103

    def test_empty_patterns(self):
        """Test that empty patterns list returns empty result."""
        # Even with no rules, empty patterns should return empty result
        result = self.engine.process_patterns([])
        self.assertEqual(len(result), 0)

    def test_no_matching_patterns(self):
        """Test that non-matching patterns don't create composite patterns."""
        # Add a test rule
        test_rule = CompositePatternRule(
            rule_id="test_rule",
            pattern_ids=["test_pattern_a", "test_pattern_b"],
            category=RiskCategory.OVERTRADING,
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
                pattern_id="unknown_pattern",  # Not matching our test_rule
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
        # Create a custom rule specifically for this test
        overtrading_rule = CompositePatternRule(
            rule_id="overtrading",
            pattern_ids=["daily_trade_limit", "cooldown_limit"],
            category=RiskCategory.OVERTRADING,
            time_window_minutes=120,  # 2 hour window
            sequence_matters=False,
             
            pattern_requirements={"daily_trade_limit": 1, "cooldown_limit": 1},
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

        result = self.engine.process_patterns(patterns, self.current_time)

        # Should have detected one composite pattern
        self.assertEqual(len(result), 1)

        # Verify the composite pattern properties
        composite = result[0]
        self.assertTrue(composite.pattern_id.startswith("composite_"))
        self.assertTrue("overtrading" in composite.pattern_id)

        # Test confidence boosting
        expected_base_confidence = (0.6 + 0.7) / 2  # Average of original confidences
        expected_boosted_confidence = expected_base_confidence + 0.1  # Default boost
        self.assertAlmostEqual(composite.confidence, expected_boosted_confidence, places=5)

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
        # Create a sequence-dependent rule specifically for this test
        sequence_rule = CompositePatternRule(
            rule_id="loss_escalation",
            pattern_ids=["consecutive_loss", "position_size_increase"],
            category=RiskCategory.SUNK_COST,
            time_window_minutes=1440,  # 24 hour window
            sequence_matters=True,  # Key setting for this test
             
            pattern_requirements={"consecutive_loss": 1, "position_size_increase": 1},
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
        result_correct = self.engine.process_patterns(correct_sequence_patterns, self.current_time)
        self.assertEqual(len(result_correct), 1)
        self.assertTrue("loss_escalation" in result_correct[0].pattern_id)

        # Test incorrect sequence
        result_incorrect = self.engine.process_patterns(incorrect_sequence_patterns, self.current_time)
        self.assertEqual(len(result_incorrect), 0)  # Should not match due to wrong sequence

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

        self.engine.add_rule(custom_rule)

        # Create patterns matching the custom rule
        patterns = [
            AtomicPattern(
                pattern_id="market_volatility",
                job_id=[self.job_id_1],
                message="High market volatility",
                severity=0.4,
                start_time=self.current_time - timedelta(minutes=30)
            ),
            AtomicPattern(
                pattern_id="rapid_trade",
                job_id=[self.job_id_2],
                message="Rapid trading detected",
                severity=0.5,
                start_time=self.current_time - timedelta(minutes=10)
            )
        ]

        result = self.engine.process_patterns(patterns, self.current_time)

        # Should have detected one composite pattern
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].pattern_id, "composite_custom_fomo_test")

        # Test confidence boosting for custom rule
        expected_base_confidence = (0.4 + 0.5) / 2  # Average of original confidences
        expected_boosted_confidence = expected_base_confidence + 0.3  # Custom boost
        self.assertAlmostEqual(result[0].confidence, expected_boosted_confidence, places=5)

    def test_pattern_with_duration(self):
        """Test detection with patterns that have both start and end times."""
        # Create a rule for testing patterns with duration
        duration_rule = CompositePatternRule(
            rule_id="duration_test",
            pattern_ids=["pattern_with_duration", "pattern_after"],
            category=RiskCategory.OVERTRADING,
            time_window_minutes=120,  # 2 hour window
             
            message="Testing patterns with duration"
        )
        self.engine.add_rule(duration_rule)
        
        patterns = [
            AtomicPattern(
                pattern_id="pattern_with_duration",
                job_id=[self.job_id_1],
                message="Pattern with explicit duration",
                severity=0.6,
                start_time=self.current_time - timedelta(hours=3),
                end_time=self.current_time - timedelta(hours=2)  # 1 hour duration
            ),
            AtomicPattern(
                pattern_id="pattern_after",
                job_id=[self.job_id_2],
                message="Pattern after the first one",
                severity=0.7,
                start_time=self.current_time - timedelta(hours=1.5)  # Starts after first pattern ends
            )
        ]

        result = self.engine.process_patterns(patterns, self.current_time)
        self.assertEqual(len(result), 1)
        self.assertIn("duration_test", result[0].pattern_id)

    def test_multiple_job_ids_combined(self):
        """Test that job IDs from source patterns are combined in composite pattern."""
        # Create a rule for job ID combination test
        job_id_rule = CompositePatternRule(
            rule_id="job_id_test",
            pattern_ids=["pattern_a", "pattern_b"],
            category=RiskCategory.FOMO,
            time_window_minutes=120,
             
            message="Testing job ID combination"
        )
        self.engine.add_rule(job_id_rule)
        
        patterns = [
            AtomicPattern(
                pattern_id="pattern_a",
                job_id=[self.job_id_1, self.job_id_2],  # Multiple job IDs
                message="Pattern with multiple job IDs",
                severity=0.6,
                start_time=self.current_time - timedelta(hours=1)
            ),
            AtomicPattern(
                pattern_id="pattern_b",
                job_id=[self.job_id_2, self.job_id_3],  # Overlapping job IDs
                message="Pattern with overlapping job IDs",
                severity=0.7,
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
        # Create a rule for testing overlapping patterns
        overlap_rule = CompositePatternRule(
            rule_id="overlap_test",
            pattern_ids=["long_duration", "overlapping"],
            category=RiskCategory.OVERTRADING,
            time_window_minutes=180,  # 3 hour window
             
            message="Testing overlapping pattern detection"
        )
        self.engine.add_rule(overlap_rule)
        
        # Create patterns that overlap in time
        patterns = [
            AtomicPattern(
                pattern_id="long_duration",
                job_id=[self.job_id_1],
                message="Long duration pattern",
                severity=0.6,
                start_time=self.current_time - timedelta(hours=3),
                end_time=self.current_time - timedelta(hours=1)  # 2-hour duration
            ),
            AtomicPattern(
                pattern_id="overlapping",
                job_id=[self.job_id_2],
                message="Pattern that overlaps with first",
                severity=0.7,
                start_time=self.current_time - timedelta(hours=2),  # Starts during first pattern
                end_time=self.current_time - timedelta(minutes=30)  # Ends after first pattern
            )
        ]

        result = self.engine.process_patterns(patterns, self.current_time)

        # Should detect the composite pattern despite overlap
        self.assertEqual(len(result), 1)
        self.assertIn("overlap_test", result[0].pattern_id)

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
        """Test that pattern requirements are correctly enforced."""
        specific_requirements_rule = CompositePatternRule(
            rule_id="specific_requirements_test",
            pattern_ids=["pattern_a", "pattern_b", "pattern_c"],
            category=RiskCategory.FOMO,
            time_window_minutes=60,
            sequence_matters=False,
            pattern_requirements={"pattern_a": 1, "pattern_b": 1, "pattern_c": 0},
            confidence_boost=0.2,
            message="Specific pattern requirements test"
        )
        multi_instance_rule = CompositePatternRule(
            rule_id="multi_instance_test",
            pattern_ids=["pattern_a", "pattern_b"],
            category=RiskCategory.FOMO,
            time_window_minutes=60,
            sequence_matters=False,
             
            pattern_requirements={"pattern_a": 2, "pattern_b": 1},
            confidence_boost=0.1,
            message="Multiple instances test"
        )

        custom_engine = PatternCompositionEngine()
        custom_engine.rules = []
        custom_engine.add_rule(specific_requirements_rule)
        custom_engine.add_rule(multi_instance_rule)

        incomplete_patterns = [
            AtomicPattern(
                pattern_id="pattern_a",
                job_id=[self.job_id_1],
                message="Pattern A detected",
                severity=0.5,
                start_time=self.current_time - timedelta(minutes=30)
            ),
            AtomicPattern(
                pattern_id="pattern_b",
                job_id=[self.job_id_2],
                message="Pattern B detected",
                severity=0.6,
                start_time=self.current_time - timedelta(minutes=15)
            )
        ]
        result_incomplete = custom_engine.process_patterns(incomplete_patterns, self.current_time)
        self.assertEqual(len(result_incomplete), 1)
        self.assertEqual(result_incomplete[0].pattern_id, "composite_specific_requirements_test")

        complete_patterns = incomplete_patterns + [
            AtomicPattern(
                pattern_id="pattern_c",
                job_id=[self.job_id_3],
                message="Pattern C detected",
                severity=0.7,
                start_time=self.current_time - timedelta(minutes=5)
            )
        ]
        result_complete = custom_engine.process_patterns(complete_patterns, self.current_time)
        self.assertEqual(len(result_complete), 1)
        self.assertEqual(result_complete[0].pattern_id, "composite_specific_requirements_test")

        # Test multi_instance_rule with insufficient patterns
        multi_patterns = [
            AtomicPattern(
                pattern_id="pattern_a",
                job_id=[self.job_id_1],
                message="Pattern A instance 1",
                severity=0.5,
                start_time=self.current_time - timedelta(minutes=30)
            ),
            AtomicPattern(
                pattern_id="pattern_a",
                job_id=[self.job_id_3],
                message="Pattern A instance 2",
                severity=0.5,
                start_time=self.current_time - timedelta(minutes=5)
            ),
            AtomicPattern(
                pattern_id="pattern_b",
                job_id=[self.job_id_3],
                message="Pattern B instance 1",
                severity=0.5,
                start_time=self.current_time - timedelta(minutes=5)
            )
        ]
        # Now should match both rules
        result_complete_multi = custom_engine.process_patterns(multi_patterns, self.current_time)
        self.assertEqual(len(result_complete_multi), 2)

    def test_multiple_instances_same_pattern_type(self):
        """Test that a rule requiring multiple instances of the same pattern type works correctly."""
        # Create a fresh engine
        engine = PatternCompositionEngine()
        engine.rules = []

        # Create a rule that requires multiple instances of the same pattern
        multi_instance_rule = CompositePatternRule(
            rule_id="cutting_profits",
            pattern_ids=["early_profit_exit"],
            category=RiskCategory.LOSS_BEHAVIOR,
            time_window_minutes=1440 * 7,  # 7 days
            pattern_requirements={"early_profit_exit": 3},  # Require 3 of the same pattern
             
            confidence_boost=0.2,
            message="Multiple early profit exits detected"
        )

        engine.add_rule(multi_instance_rule)

        # Create 5 patterns of the same type over 5 days
        patterns = [
            AtomicPattern(
                pattern_id="early_profit_exit",
                message=f"Early profit exit on BTC ({i + 1})",
                severity=0.5,
                job_id=[1000 + i],
                start_time=self.current_time - timedelta(days=i),
            )
            for i in range(5)  # Create 5 patterns over 5 days
        ]

        # Add a different pattern type to ensure proper filtering
        patterns.append(
            AtomicPattern(
                pattern_id="position_unrealized_pnl_threshold",
                message="Position has significant unrealized loss",
                severity=0.7,
                job_id=[2000],
                start_time=self.current_time - timedelta(days=2),
            )
        )

        # Process the patterns with the engine
        composite_patterns = engine.process_patterns(patterns, current_time=self.current_time)

        # Assertions
        # 1. Check that we got exactly one composite pattern
        self.assertEqual(len(composite_patterns), 1, "Should detect exactly one composite pattern")

        # 2. Check that the composite pattern has the correct properties
        composite = composite_patterns[0]
        self.assertEqual(composite.pattern_id, "composite_cutting_profits")
        self.assertTrue(composite.is_composite)

        # 3. Check that the composite pattern contains at least 3 consumed patterns
        consumed_patterns = [p for p in patterns if p.consumed]
        self.assertGreaterEqual(len(consumed_patterns), 3, "At least 3 patterns should be consumed")

        # 4. Verify all consumed patterns are of type "early_profit_exit"
        for p in consumed_patterns:
            self.assertEqual(p.pattern_id, "early_profit_exit", "Only early_profit_exit patterns should be consumed")

        # 5. Check confidence calculation
        # Base confidence should be average of consumed patterns (all 0.5) + boost (0.2)
        expected_confidence = 0.5 + 0.2
        self.assertAlmostEqual(composite.confidence, expected_confidence, places=2, 
                              msg="Confidence calculation incorrect")

        # 6. Check time boundaries
        # Start time should be the earliest consumed pattern
        # End time should be the latest consumed pattern
        consumed_start_times = [p.start_time for p in consumed_patterns]
        consumed_end_times = [p.end_time or p.start_time for p in consumed_patterns]

        self.assertEqual(composite.start_time, min(consumed_start_times))
        self.assertEqual(composite.end_time, max(consumed_end_times))

        # 7. Check that the pattern with different ID wasn't consumed
        for p in patterns:
            if p.pattern_id == "position_unrealized_pnl_threshold":
                self.assertFalse(p.consumed, "Patterns of different types should not be consumed")

    def test_greedy_consumption_true(self):
        """Test that greedy_consumption=True consumes all available patterns of the required type."""
        engine = PatternCompositionEngine()
        engine.rules = []
        greedy_rule = CompositePatternRule(
            rule_id="greedy_test",
            pattern_ids=["multi_pattern"],
            category=RiskCategory.FOMO,
            time_window_minutes=1440,
            pattern_requirements={"multi_pattern": 2},
            confidence_boost=0.2,
            message="Greedy pattern consumption test",
            greedy_consumption=True
        )
        engine.add_rule(greedy_rule)

        patterns = [
            AtomicPattern(
                pattern_id="multi_pattern",
                message=f"Multi pattern instance {i+1}",
                severity=0.5,
                job_id=[1000 + i],
                start_time=self.current_time - timedelta(hours=i*2)
            )
            for i in range(5)
        ]

        results = engine.process_patterns(patterns, self.current_time)
        self.assertEqual(len(results), 1)
        
        # Find the greedy result
        greedy_result = next((r for r in results if r.pattern_id == "composite_greedy_test"), None)
        self.assertIsNotNone(greedy_result, "Greedy rule should have matched")
        
        # Count consumed patterns in the result
        greedy_components = greedy_result.details["components"]
        
        # Greedy rule should consume all 5 patterns
        self.assertEqual(len(greedy_components), 5, "Greedy rule should consume all available patterns")

    def test_greedy_consumption_false(self):
        """Test that greedy_consumption=False only consumes required number of patterns."""
        engine = PatternCompositionEngine()
        engine.rules = []

        non_greedy_rule = CompositePatternRule(
            rule_id="non_greedy_test",
            pattern_ids=["multi_pattern"],
            category=RiskCategory.FOMO,
            time_window_minutes=1440,
            pattern_requirements={"multi_pattern": 2},
            confidence_boost=0.2,
            message="Non-greedy pattern consumption test",
            greedy_consumption=False
        )

        engine.add_rule(non_greedy_rule)

        patterns = [
            AtomicPattern(
                pattern_id="multi_pattern",
                message=f"Multi pattern instance {i+1}",
                severity=0.5,
                job_id=[1000 + i],
                start_time=self.current_time - timedelta(hours=i*2)
            )
            for i in range(5)
        ]

        results = engine.process_patterns(patterns, self.current_time)
        self.assertEqual(len(results), 1)
        
        # Find the non-greedy result
        non_greedy_result = next((r for r in results if r.pattern_id == "composite_non_greedy_test"), None)
        self.assertIsNotNone(non_greedy_result, "Non-greedy rule should have matched")
        
        # Count consumed patterns in the result
        non_greedy_components = non_greedy_result.details["components"]
        
        # Non-greedy rule should consume only 2 patterns (as required)
        self.assertEqual(len(non_greedy_components), 2, "Non-greedy rule should consume only required number of patterns")

    def test_pattern_scoring_in_window(self):
        """Test that pattern scoring within a time window works correctly."""
        engine = PatternCompositionEngine()
        engine.rules = []

        window_rule = CompositePatternRule(
            rule_id="window_scoring_test",
            pattern_ids=["pattern_type_a", "pattern_type_b"],
            category=RiskCategory.FOMO,
            time_window_minutes=120,  # 2-hour window
             
            confidence_boost=0.1,
            message="Test window pattern scoring"
        )
        engine.add_rule(window_rule)
        
        # Pattern with higher confidence but less overlap with the time window
        high_confidence_pattern = AtomicPattern(
            pattern_id="pattern_type_a",
            message="High confidence pattern",
            severity=0.9,  # High confidence
            job_id=[1001],
            start_time=self.current_time - timedelta(hours=3),
            end_time=self.current_time - timedelta(hours=2.8)
        )

        full_overlap_pattern = AtomicPattern(
            pattern_id="pattern_type_a",
            message="Full overlap pattern",
            severity=0.5,  # Lower confidence
            job_id=[1002],
            start_time=self.current_time - timedelta(hours=2),
            end_time=self.current_time  # Full overlap with window
        )
        
        # Pattern of second type needed to complete the rule match
        second_type_pattern = AtomicPattern(
            pattern_id="pattern_type_b",
            message="Second pattern type",
            severity=0.6,
            job_id=[1003],
            start_time=self.current_time - timedelta(hours=1)
        )
        
        # Process patterns - the full overlap pattern should be preferred even with lower confidence
        results = engine.process_patterns(
            [high_confidence_pattern, full_overlap_pattern, second_type_pattern], 
            self.current_time
        )
        
        # Should have one result
        self.assertEqual(len(results), 1)
        
        # Find the component pattern types in the result
        component_pattern_ids = [comp["pattern_type"] for comp in results[0].details["components"]]
        component_pattern_ids.sort()  # Sort for consistent comparison
        
        # The result should include the full overlap pattern (pattern_type_a) and the second_type_pattern
        expected_types = ["pattern_type_a", "pattern_type_b"]
        expected_types.sort()
        self.assertEqual(component_pattern_ids, expected_types)
        
        # Verify that the full_overlap_pattern was consumed (it has better overlap despite lower confidence)
        self.assertTrue(full_overlap_pattern.consumed, "Full overlap pattern should be consumed")
        
        # High confidence pattern might not be consumed if it has worse overlap
        # This is implementation-dependent, so we don't assert on it

    def test_patterns_within_time_window(self):
        reference_time = datetime(2024, 1, 1, 12, 0)
        
        window_rule = CompositePatternRule(
            rule_id="time_window_test",
            pattern_ids=["pattern_a", "pattern_b"],
            category=RiskCategory.FOMO,
            time_window_minutes=60,
             
            pattern_requirements={"pattern_a": 1, "pattern_b": 1},
            message="Patterns within time window"
        )
        self.engine.add_rule(window_rule)
        
        patterns = [
            AtomicPattern(
                pattern_id="pattern_a",
                job_id=[self.job_id_1],
                message="Pattern A",
                severity=0.6,
                start_time=reference_time - timedelta(minutes=30),
                end_time=reference_time - timedelta(minutes=20)
            ),
            AtomicPattern(
                pattern_id="pattern_b",
                job_id=[self.job_id_2],
                message="Pattern B",
                severity=0.7,
                start_time=reference_time - timedelta(minutes=10),
                end_time=reference_time
            )
        ]
        
        result = self.engine.process_patterns(patterns, reference_time)
        self.assertEqual(len(result), 1, "Should find one composite pattern when patterns are within time window")
        self.assertEqual(len(result[0].details["components"]), 2, "Composite pattern should contain both input patterns")

    def test_patterns_outside_time_window(self):
        reference_time = datetime(2024, 1, 1, 12, 0)
        
        window_rule = CompositePatternRule(
            rule_id="time_window_test",
            pattern_ids=["pattern_a", "pattern_b"],
            category=RiskCategory.FOMO,
            time_window_minutes=30,
             
            pattern_requirements={"pattern_a": 1, "pattern_b": 1},
            message="Patterns outside time window"
        )
        self.engine.add_rule(window_rule)
        
        patterns = [
            AtomicPattern(
                pattern_id="pattern_a",
                job_id=[self.job_id_1],
                message="Pattern A",
                severity=0.6,
                start_time=reference_time - timedelta(minutes=45),
                end_time=reference_time - timedelta(minutes=35)
            ),
            AtomicPattern(
                pattern_id="pattern_b",
                job_id=[self.job_id_2],
                message="Pattern B",
                severity=0.7,
                start_time=reference_time - timedelta(minutes=10),
                end_time=reference_time
            )
        ]
        
        result = self.engine.process_patterns(patterns, reference_time)
        self.assertEqual(len(result), 0, "Should not find composite pattern when patterns are outside time window")

    def test_patterns_touching_time_window_boundary(self):
        reference_time = datetime(2024, 1, 1, 12, 0)
        
        window_rule = CompositePatternRule(
            rule_id="time_window_test",
            pattern_ids=["pattern_a", "pattern_b"],
            category=RiskCategory.FOMO,
            time_window_minutes=30,
             
            pattern_requirements={"pattern_a": 1, "pattern_b": 1},
            message="Patterns touching time window boundary"
        )
        self.engine.add_rule(window_rule)
        
        patterns = [
            AtomicPattern(
                pattern_id="pattern_a",
                job_id=[self.job_id_1],
                message="Pattern A",
                severity=0.6,
                start_time=reference_time - timedelta(minutes=30),
                end_time=reference_time - timedelta(minutes=20)
            ),
            AtomicPattern(
                pattern_id="pattern_b",
                job_id=[self.job_id_2],
                message="Pattern B",
                severity=0.7,
                start_time=reference_time - timedelta(minutes=10),
                end_time=reference_time
            )
        ]
        
        result = self.engine.process_patterns(patterns, reference_time)
        self.assertEqual(len(result), 1, "Should find composite pattern when patterns touch time window boundary")
        self.assertEqual(len(result[0].details["components"]), 2, "Composite pattern should contain both input patterns")

    def test_patterns_with_missing_end_times(self):
        reference_time = datetime(2024, 1, 1, 12, 0)
        
        window_rule = CompositePatternRule(
            rule_id="time_window_test",
            pattern_ids=["pattern_a", "pattern_b"],
            category=RiskCategory.FOMO,
            time_window_minutes=30,
             
            pattern_requirements={"pattern_a": 1, "pattern_b": 1},
            message="Patterns with missing end times"
        )
        self.engine.add_rule(window_rule)
        
        patterns = [
            AtomicPattern(
                pattern_id="pattern_a",
                job_id=[self.job_id_1],
                message="Pattern A",
                severity=0.6,
                start_time=reference_time - timedelta(minutes=20)
            ),
            AtomicPattern(
                pattern_id="pattern_b",
                job_id=[self.job_id_2],
                message="Pattern B",
                severity=0.7,
                start_time=reference_time - timedelta(minutes=10)
            )
        ]
        
        result = self.engine.process_patterns(patterns, reference_time)
        self.assertEqual(len(result), 1, "Should find composite pattern when patterns have missing end times")
        self.assertEqual(len(result[0].details["components"]), 2, "Composite pattern should contain both input patterns")

    def test_patterns_with_overlapping_times(self):
        reference_time = datetime(2024, 1, 1, 12, 0)
        
        window_rule = CompositePatternRule(
            rule_id="time_window_test",
            pattern_ids=["pattern_a", "pattern_b"],
            category=RiskCategory.FOMO,
            time_window_minutes=30,
             
            pattern_requirements={"pattern_a": 1, "pattern_b": 1},
            message="Patterns with overlapping times"
        )
        self.engine.add_rule(window_rule)
        
        patterns = [
            AtomicPattern(
                pattern_id="pattern_a",
                job_id=[self.job_id_1],
                message="Pattern A",
                severity=0.6,
                start_time=reference_time - timedelta(minutes=25),
                end_time=reference_time - timedelta(minutes=15)
            ),
            AtomicPattern(
                pattern_id="pattern_b",
                job_id=[self.job_id_2],
                message="Pattern B",
                severity=0.7,
                start_time=reference_time - timedelta(minutes=20),
                end_time=reference_time - timedelta(minutes=10)
            )
        ]
        
        result = self.engine.process_patterns(patterns, reference_time)
        self.assertEqual(len(result), 1, "Should find composite pattern when patterns have overlapping times")
        self.assertEqual(len(result[0].details["components"]), 2, "Composite pattern should contain both input patterns")


if __name__ == '__main__':
    unittest.main()
