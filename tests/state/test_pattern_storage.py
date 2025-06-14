import unittest
from datetime import datetime, timezone
from src.models.risk_models import AtomicPattern
from src.state.pattern_storage import PatternStorage


class TestPatternStorage(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.storage = PatternStorage()
        self.user_id = 1

    def tearDown(self):
        """Clean up test fixtures after each test method."""
        self.storage.clear_all_patterns()

    def test_store_patterns_preserves_state(self):
        """Test that store_patterns preserves existing patterns when adding new ones."""
        # Create initial patterns
        initial_patterns = [
            AtomicPattern(
                pattern_id="test_pattern_1",
                message="Test pattern 1",
                severity=0.5,
                unique=False
            ),
            AtomicPattern(
                pattern_id="test_pattern_2",
                message="Test pattern 2",
                severity=0.7,
                unique=True
            )
        ]

        # Store initial patterns
        self.storage.store_patterns(self.user_id, initial_patterns)

        # Create new patterns
        new_patterns = [
            AtomicPattern(
                pattern_id="test_pattern_3",
                message="Test pattern 3",
                severity=0.6,
                unique=False
            )
        ]

        # Store new patterns
        self.storage.store_patterns(self.user_id, new_patterns)

        # Retrieve all patterns
        stored_patterns = self.storage.get_user_patterns(self.user_id)

        # Verify that all patterns are preserved
        self.assertEqual(len(stored_patterns), 3, f"Expected 3 patterns, got {len(stored_patterns)}")

        # Verify that both initial and new patterns are present
        pattern_ids = {p.pattern_id for p in stored_patterns}
        self.assertIn("test_pattern_1", pattern_ids, "Initial pattern 1 not found")
        self.assertIn("test_pattern_2", pattern_ids, "Initial pattern 2 not found")
        self.assertIn("test_pattern_3", pattern_ids, "New pattern not found")

    def test_store_patterns_handles_unique_patterns(self):
        """Test that store_patterns correctly handles unique patterns by replacing them."""
        initial_pattern = AtomicPattern(
            pattern_id="unique_pattern",
            message="Initial unique pattern",
            severity=0.5,
            unique=True
        )
        self.storage.store_patterns(self.user_id, [initial_pattern])

        new_pattern = AtomicPattern(
            pattern_id="unique_pattern",
            message="New unique pattern",
            severity=0.7,
            unique=True
        )
        self.storage.store_patterns(self.user_id, [new_pattern])

        stored_patterns = self.storage.get_user_patterns(self.user_id)

        self.assertEqual(len(stored_patterns), 1, "Expected 1 pattern, got {len(stored_patterns)}")
        self.assertEqual(stored_patterns[0].message, "New unique pattern", "Pattern was not replaced")

    def test_store_patterns_handles_non_unique_patterns(self):
        """Test that store_patterns correctly handles non-unique patterns by keeping all instances."""
        initial_pattern = AtomicPattern(
            pattern_id="non_unique_pattern",
            message="Initial non-unique pattern",
            severity=0.5,
            unique=False
        )
        self.storage.store_patterns(self.user_id, [initial_pattern])

        new_pattern = AtomicPattern(
            pattern_id="non_unique_pattern",
            message="New non-unique pattern",
            severity=0.7,
            unique=False
        )
        self.storage.store_patterns(self.user_id, [new_pattern])

        # Retrieve patterns
        stored_patterns = self.storage.get_user_patterns(self.user_id)

        # Verify that both patterns exist
        self.assertEqual(len(stored_patterns), 2, "Expected 2 patterns, got {len(stored_patterns)}")
        pattern_messages = {p.message for p in stored_patterns}
        self.assertIn("Initial non-unique pattern", pattern_messages, "Initial pattern not found")
        self.assertIn("New non-unique pattern", pattern_messages, "New pattern not found")

    def test_store_patterns_handles_position_patterns(self):
        """Test that store_patterns correctly handles patterns with position keys."""
        # Case 1: Unique patterns with different position keys should both be saved
        initial_pattern = AtomicPattern(
            pattern_id="position_pattern",
            message="Initial position pattern",
            severity=0.5,
            unique=True,
            position_key="BYBIT_BTC"
        )
        self.storage.store_patterns(self.user_id, [initial_pattern])

        # Debug: Check what's stored after first pattern
        first_stored = self.storage.get_user_patterns(self.user_id)
        print(f"After first pattern: {len(first_stored)} patterns")
        for p in first_stored:
            print(f"Pattern: {p.pattern_id}, position_key: {p.position_key}")

        new_pattern = AtomicPattern(
            pattern_id="position_pattern",
            message="New position pattern",
            severity=0.7,
            unique=True,
            position_key="BYBIT_ETH"
        )
        self.storage.store_patterns(self.user_id, [new_pattern])

        # Debug: Check what's stored after second pattern
        second_stored = self.storage.get_user_patterns(self.user_id)
        print(f"After second pattern: {len(second_stored)} patterns")
        for p in second_stored:
            print(f"Pattern: {p.pattern_id}, position_key: {p.position_key}")

        stored_patterns = self.storage.get_user_patterns(self.user_id)

        # Debug: Check final state
        print(f"Final state: {len(stored_patterns)} patterns")
        for p in stored_patterns:
            print(f"Pattern: {p.pattern_id}, position_key: {p.position_key}")

        self.assertEqual(len(stored_patterns), 2, f"Expected 2 patterns, got {len(stored_patterns)}")
        position_keys = {p.position_key for p in stored_patterns}
        self.assertIn("BYBIT_BTC", position_keys, "BTC pattern not found")
        self.assertIn("BYBIT_ETH", position_keys, "ETH pattern not found")


def test_store_patterns_handles_job_patterns(self):
    """Test that store_patterns correctly handles patterns with job IDs."""
    # Case 1: Non-unique patterns with same job_id should both be saved
    initial_pattern = AtomicPattern(
        pattern_id="limit_force_job",
        message="Job 105 has force parameter.",
        severity=1.0,
        unique=False,
        job_id=[105],
        start_time=datetime(2025, 6, 1, 14, 0, 44, tzinfo=timezone.utc)
    )
    self.storage.store_patterns(self.user_id, [initial_pattern])

    new_pattern = AtomicPattern(
        pattern_id="limit_force_job",
        message="Job 105 has force parameter.",
        severity=1.0,
        unique=False,
        job_id=[105],
        start_time=datetime(2025, 6, 1, 14, 1, 8, tzinfo=timezone.utc)
    )
    self.storage.store_patterns(self.user_id, [new_pattern])

    stored_patterns = self.storage.get_user_patterns(self.user_id)

    # Since patterns are non-unique, both should be saved
    self.assertEqual(len(stored_patterns), 2, f"Expected 2 patterns, got {len(stored_patterns)}")

    # Verify both patterns exist with correct job_ids
    job_patterns = [p for p in stored_patterns if p.pattern_id == "limit_force_job"]
    self.assertEqual(len(job_patterns), 2, "Expected 2 limit_force_job patterns")

    # Verify job_ids are preserved
    for pattern in job_patterns:
        self.assertEqual(pattern.job_id, [105], "Job ID not preserved")


if __name__ == '__main__':
    unittest.main()
