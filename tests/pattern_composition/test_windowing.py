import unittest
from datetime import datetime, timedelta

from src.models import RiskCategory
from src.risk.pattern_composition import PatternCompositionEngine, CompositePatternRule
from src.risk.aggregation_factory import AtomicPattern


class TestPatternCompositionTimeWindow(unittest.TestCase):
    def setUp(self):
        self.engine = PatternCompositionEngine()
        self.job_id_1 = 1
        self.job_id_2 = 2
        self.reference_time = datetime(2024, 1, 1, 12, 0)

    def test_patterns_within_time_window(self):
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
                start_time=self.reference_time - timedelta(minutes=30),
                end_time=self.reference_time - timedelta(minutes=20)
            ),
            AtomicPattern(
                pattern_id="pattern_b",
                job_id=[self.job_id_2],
                message="Pattern B",
                severity=0.7,
                start_time=self.reference_time - timedelta(minutes=10),
                end_time=self.reference_time
            )
        ]

        result = self.engine.process_patterns(patterns, self.reference_time)
        self.assertEqual(len(result), 1, "Should find one composite pattern when patterns are within time window")
        self.assertEqual(len(result[0].details["components"]), 2,
                         "Composite pattern should contain both input patterns")

    def test_patterns_outside_time_window(self):
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
                start_time=self.reference_time - timedelta(minutes=45),
                end_time=self.reference_time - timedelta(minutes=35)
            ),
            AtomicPattern(
                pattern_id="pattern_b",
                job_id=[self.job_id_2],
                message="Pattern B",
                severity=0.7,
                start_time=self.reference_time - timedelta(minutes=10),
                end_time=self.reference_time
            )
        ]

        result = self.engine.process_patterns(patterns, self.reference_time)
        self.assertEqual(len(result), 0, "Should not find composite pattern when patterns are outside time window")

    def test_patterns_touching_time_window_boundary(self):
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
                start_time=self.reference_time - timedelta(minutes=30),
                end_time=self.reference_time - timedelta(minutes=20)
            ),
            AtomicPattern(
                pattern_id="pattern_b",
                job_id=[self.job_id_2],
                message="Pattern B",
                severity=0.7,
                start_time=self.reference_time - timedelta(minutes=10),
                end_time=self.reference_time
            )
        ]

        result = self.engine.process_patterns(patterns, self.reference_time)
        self.assertEqual(len(result), 1, "Should find composite pattern when patterns touch time window boundary")
        self.assertEqual(len(result[0].details["components"]), 2,
                         "Composite pattern should contain both input patterns")

    def test_patterns_with_missing_end_times(self):
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
                start_time=self.reference_time - timedelta(minutes=20)
            ),
            AtomicPattern(
                pattern_id="pattern_b",
                job_id=[self.job_id_2],
                message="Pattern B",
                severity=0.7,
                start_time=self.reference_time - timedelta(minutes=10)
            )
        ]

        result = self.engine.process_patterns(patterns, self.reference_time)
        self.assertEqual(len(result), 1, "Should find composite pattern when patterns have missing end times")
        self.assertEqual(len(result[0].details["components"]), 2,
                         "Composite pattern should contain both input patterns")

    def test_patterns_with_overlapping_times(self):
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
                start_time=self.reference_time - timedelta(minutes=25),
                end_time=self.reference_time - timedelta(minutes=15)
            ),
            AtomicPattern(
                pattern_id="pattern_b",
                job_id=[self.job_id_2],
                message="Pattern B",
                severity=0.7,
                start_time=self.reference_time - timedelta(minutes=20),
                end_time=self.reference_time - timedelta(minutes=10)
            )
        ]

        result = self.engine.process_patterns(patterns, self.reference_time)
        self.assertEqual(len(result), 1, "Should find composite pattern when patterns have overlapping times")
        self.assertEqual(len(result[0].details["components"]), 2,
                         "Composite pattern should contain both input patterns")


if __name__ == '__main__':
    unittest.main()
