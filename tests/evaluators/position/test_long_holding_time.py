import unittest
from datetime import datetime, timezone, timedelta
from typing import Dict, List

from src.models import Position
from src.models.position_models import PositionUpdateType
from src.risk.evaluators.positions_evaluator import PositionEvaluator
from src.state.state_manager import StateManager


def create_position(
        venue: str,
        symbol: str,
        side: str,
        entry_price: float,
        mark_price: float,
        update_type: str,
        timestamp: datetime = None,
        qty: float = 1.0,
        user_id: int = 12345
) -> Position:
    """Create a position object with common default values"""
    if timestamp is None:
        timestamp = datetime.now(timezone.utc)
        
    return Position(
        venue=venue,
        symbol=symbol,
        side=side,
        qty=qty,
        usdt_amt=1000.0,
        entry_price=entry_price,
        mark_price=mark_price,
        liquidation_price=None,
        unrealized_pnl=0.0,
        cur_realized_pnl=0.0,
        cum_realized_pnl=0.0,
        leverage=1.0,
        timestamp=timestamp,
        account_name="test_account",
        user_id=user_id,
        update_type=update_type
    )


class TestLongHoldingTime(unittest.TestCase):
    """Test case for the check_long_holding_time function in PositionEvaluator"""

    def setUp(self):
        """Set up the test environment before each test"""
        self.state_manager = StateManager()
        self.evaluator = PositionEvaluator(self.state_manager)
        self.evaluator.LONG_HOLDING_DAYS_THRESHOLD = 7  # 7 days threshold
        self.user_id = 12345
        self.now = datetime.now(timezone.utc)

    def test_no_positions(self):
        """Test that no patterns are created when there are no positions"""
        position_histories = {}

        patterns = self.evaluator.check_long_holding_time(self.user_id, position_histories)
        self.assertEqual(len(patterns), 0, "Expected no patterns when no positions exist")

    def test_closed_position(self):
        """Test that no patterns are created for closed positions"""
        closed_pos = create_position(
            venue="BINANCE",
            symbol="BTC",
            side="Buy",
            entry_price=50000.0,
            mark_price=51000.0,
            update_type=PositionUpdateType.CLOSED,
            qty=0.0  # Closed position (zero quantity)
        )

        position_histories = {
            'BINANCE_BTC': [closed_pos]
        }

        patterns = self.evaluator.check_long_holding_time(self.user_id, position_histories)
        self.assertEqual(len(patterns), 0, "Expected no patterns for closed positions")

    def test_new_position(self):
        """Test that no patterns are created for newly opened positions"""
        recent_timestamp = self.now - timedelta(days=1)
        recent_pos = create_position(
            venue="BINANCE",
            symbol="BTC",
            side="Buy",
            entry_price=50000.0,
            mark_price=51000.0,
            update_type=PositionUpdateType.INCREASED,
            timestamp=recent_timestamp
        )

        position_histories = {
            'BINANCE_BTC': [recent_pos]
        }

        patterns = self.evaluator.check_long_holding_time(self.user_id, position_histories)
        self.assertEqual(len(patterns), 0, "Expected no patterns for new positions")

    def test_old_position(self):
        """Test detection of positions held for too long"""
        # Create a position that was opened 10 days ago (exceeds 7-day threshold)
        old_timestamp = self.now - timedelta(days=10)
        current_pos = create_position(
            venue="BINANCE",
            symbol="BTC",
            side="Buy",
            entry_price=50000.0,
            mark_price=51000.0,
            update_type=PositionUpdateType.SNAPSHOT,
            timestamp=self.now
        )
        
        entry_pos = create_position(
            venue="BINANCE",
            symbol="BTC",
            side="Buy",
            entry_price=50000.0,
            mark_price=50000.0,
            update_type=PositionUpdateType.INCREASED,
            timestamp=old_timestamp  # Old entry timestamp
        )

        position_histories = {
            'BINANCE_BTC': [current_pos, entry_pos]  # Newest first
        }

        patterns = self.evaluator.check_long_holding_time(self.user_id, position_histories)

        # Verify we got 1 pattern
        self.assertEqual(len(patterns), 1, "Expected 1 pattern for old position")

        # Verify pattern details
        pattern = patterns[0]
        self.assertEqual(pattern.pattern_id, "position_long_holding_time")
        self.assertEqual(pattern.details["symbol"], "BTC")
        self.assertEqual(pattern.details["venue"], "BINANCE")
        self.assertAlmostEqual(pattern.details["holding_days"], 10.0, delta=0.1)

    def test_multiple_entries(self):
        """Test with multiple position entries to ensure first entry is used"""
        # Create position history with multiple entries
        
        # Current snapshot
        current_pos = create_position(
            venue="BINANCE",
            symbol="ETH",
            side="Buy",
            entry_price=3100.0,  # Average entry price after multiple entries
            mark_price=3200.0,
            update_type=PositionUpdateType.SNAPSHOT,
            timestamp=self.now  # Current time
        )
        
        # Second entry 5 days ago (not old enough)
        second_entry = create_position(
            venue="BINANCE",
            symbol="ETH",
            side="Buy",
            entry_price=3200.0,
            mark_price=3200.0,
            update_type=PositionUpdateType.INCREASED,
            timestamp=self.now - timedelta(days=5)
        )
        
        # First entry 15 days ago (exceeds threshold)
        first_entry = create_position(
            venue="BINANCE",
            symbol="ETH",
            side="Buy",
            entry_price=3000.0,
            mark_price=3000.0,
            update_type=PositionUpdateType.INCREASED,
            timestamp=self.now - timedelta(days=15)
        )
        
        position_histories = {
            'BINANCE_ETH': [current_pos, second_entry, first_entry]  # Newest first
        }
        
        patterns = self.evaluator.check_long_holding_time(self.user_id, position_histories)
        
        # Should detect based on the first entry's timestamp
        self.assertEqual(len(patterns), 1, "Expected 1 pattern based on first entry time")
        self.assertAlmostEqual(patterns[0].details["holding_days"], 15.0, delta=0.1)

    def test_snapshot_only(self):
        """Test when history only has snapshots without explicit entry events"""
        # Create position history with only snapshots
        
        # Current snapshot
        current_pos = create_position(
            venue="BINANCE",
            symbol="SOL",
            side="Buy",
            entry_price=100.0,
            mark_price=105.0,
            update_type=PositionUpdateType.SNAPSHOT,
            timestamp=self.now
        )
        
        # Older snapshot
        old_snapshot = create_position(
            venue="BINANCE",
            symbol="SOL",
            side="Buy",
            entry_price=100.0,
            mark_price=102.0,
            update_type=PositionUpdateType.SNAPSHOT,
            timestamp=self.now - timedelta(days=8)  # Just over threshold
        )
        
        position_histories = {
            'BINANCE_SOL': [current_pos, old_snapshot]  # Newest first
        }
        
        patterns = self.evaluator.check_long_holding_time(self.user_id, position_histories)
        
        # Should detect based on the oldest snapshot time
        self.assertEqual(len(patterns), 1, "Expected 1 pattern based on oldest snapshot time")
        self.assertAlmostEqual(patterns[0].details["holding_days"], 8.0, delta=0.1)

    def test_mixed_history(self):
        """Test with a mix of update types in history"""
        position_histories = {}
        
        # Create a complex history with various update types
        current_time = self.now
        
        # Current snapshot (newest)
        current_snapshot = create_position(
            venue="BINANCE",
            symbol="BTC",
            side="Buy",
            entry_price=50000.0,
            mark_price=51000.0,
            update_type=PositionUpdateType.SNAPSHOT,
            timestamp=current_time
        )
        
        # A decrease event 1 day ago
        decrease_event = create_position(
            venue="BINANCE",
            symbol="BTC",
            side="Buy",
            entry_price=50000.0,
            mark_price=50500.0,
            update_type=PositionUpdateType.DECREASED,
            timestamp=current_time - timedelta(days=1)
        )
        
        # An increase event 3 days ago
        increase_event = create_position(
            venue="BINANCE",
            symbol="BTC",
            side="Buy",
            entry_price=50000.0,
            mark_price=50200.0,
            update_type=PositionUpdateType.INCREASED,
            timestamp=current_time - timedelta(days=3)
        )
        
        # Initial entry 10 days ago
        initial_entry = create_position(
            venue="BINANCE",
            symbol="BTC",
            side="Buy",
            entry_price=50000.0,
            mark_price=50000.0,
            update_type=PositionUpdateType.INCREASED,
            timestamp=current_time - timedelta(days=10)
        )
        
        position_histories["BINANCE_BTC"] = [
            current_snapshot, 
            decrease_event, 
            increase_event, 
            initial_entry
        ]
        
        patterns = self.evaluator.check_long_holding_time(self.user_id, position_histories)
        
        # Should detect one pattern based on the initial entry
        self.assertEqual(len(patterns), 1, "Expected 1 pattern for mixed history")
        self.assertAlmostEqual(patterns[0].details["holding_days"], 10.0, delta=0.1)


if __name__ == "__main__":
    unittest.main() 