import unittest
from datetime import datetime, timezone
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
        user_id: int = 12345
) -> Position:
    """Create a position object with common default values"""
    return Position(
        venue=venue,
        symbol=symbol,
        side=side,
        qty=1.0,
        usdt_amt=1000.0,
        entry_price=entry_price,
        mark_price=mark_price,
        liquidation_price=None,
        unrealized_pnl=0.0,
        cur_realized_pnl=0.0,
        cum_realized_pnl=0.0,
        leverage=1.0,
        timestamp=datetime.now(timezone.utc),
        account_name="test_account",
        user_id=user_id,
        update_type=update_type
    )


class TestEarlyProfitExit(unittest.TestCase):
    """Test case for the early_profit_exit function in PositionEvaluator"""

    def setUp(self):
        """Set up the test environment before each test"""
        self.state_manager = StateManager()
        self.evaluator = PositionEvaluator(self.state_manager)
        self.user_id = 12345

    def test_no_early_profit_exit(self):
        """Test that no patterns are created when there are no early profit exits"""
        position_histories = {}

        patterns = self.evaluator.check_early_profit_exit(self.user_id, position_histories)
        self.assertEqual(len(patterns), 0, "Expected no patterns when no history exists")

    def test_early_profit_exit_long_position(self):
        """Test detection of early profit exit on a long position"""
        binance_btc_pos = create_position(
            venue="BINANCE",
            symbol="BTC",
            side="Buy",
            entry_price=50000.0,
            mark_price=51000.0,  # 2% profit
            update_type=PositionUpdateType.DECREASED
        )

        position_histories = {
            'BINANCE_BTC': [binance_btc_pos]
        }

        patterns = self.evaluator.check_early_profit_exit(self.user_id, position_histories)

        self.assertEqual(len(patterns), 1, "Expected 1 pattern for early profit exit")

        pattern = patterns[0]
        self.assertEqual(pattern.pattern_id, "position_early_profit_exit")
        self.assertEqual(pattern.details["symbol"], "BTC")
        self.assertEqual(pattern.details["venue"], "BINANCE")
        self.assertAlmostEqual(pattern.details["profit_pct"], 0.02, places=2)

    def test_early_profit_exit_short_position(self):
        """Test detection of early profit exit on a short position"""
        bybit_eth_pos = create_position(
            venue="BYBIT",
            symbol="ETH",
            side="Sell",
            entry_price=3000.0,
            mark_price=2910.0,  # 3% profit for short
            update_type=PositionUpdateType.DECREASED
        )

        position_histories = {
            'BYBIT_ETH': [bybit_eth_pos]
        }

        patterns = self.evaluator.check_early_profit_exit(self.user_id, position_histories)

        self.assertEqual(len(patterns), 1, "Expected 1 pattern for early profit exit")

        pattern = patterns[0]
        self.assertEqual(pattern.pattern_id, "position_early_profit_exit")
        self.assertEqual(pattern.details["symbol"], "ETH")
        self.assertEqual(pattern.details["venue"], "BYBIT")
        self.assertAlmostEqual(pattern.details["profit_pct"], 0.03, places=2)

    def test_no_pattern_for_large_profit(self):
        """Test that no patterns are created for positions with large profits"""
        large_profit_pos = create_position(
            venue="BINANCE",
            symbol="SOL",
            side="Buy",
            entry_price=100.0,
            mark_price=110.0,  # 10% profit
            update_type=PositionUpdateType.DECREASED
        )

        position_histories = {
            'BINANCE_SOL': [large_profit_pos]
        }

        patterns = self.evaluator.check_early_profit_exit(self.user_id, position_histories)

        self.assertEqual(len(patterns), 0, "Expected no patterns for large profit exit")

    def test_no_pattern_for_loss(self):
        """Test that no patterns are created for positions with losses"""
        loss_pos = create_position(
            venue="BINANCE",
            symbol="AVAX",
            side="Buy",
            entry_price=20.0,
            mark_price=19.0,  # 5% loss
            update_type=PositionUpdateType.DECREASED
        )

        position_histories = {
            'BINANCE_AVAX': [loss_pos]
        }

        patterns = self.evaluator.check_early_profit_exit(self.user_id, position_histories)
        self.assertEqual(len(patterns), 0, "Expected no patterns for loss exit")

    def test_no_pattern_for_non_exit_updates(self):
        """Test that no patterns are created for position updates that are not exits"""
        snapshot_pos = create_position(
            venue="BINANCE",
            symbol="BTC",
            side="Buy",
            entry_price=50000.0,
            mark_price=51000.0,  # 2% profit
            update_type=PositionUpdateType.SNAPSHOT
        )

        position_histories = {
            'BINANCE_BTC': [snapshot_pos]
        }

        patterns = self.evaluator.check_early_profit_exit(self.user_id, position_histories)

        self.assertEqual(len(patterns), 0, "Expected no patterns for non-exit updates")

    def test_multiple_positions(self):
        """Test detection with multiple positions, only some of which show early profit exits"""
        position_histories = {}

        # Position 1: Early profit exit (should trigger pattern)
        early_exit_pos = create_position(
            venue="BINANCE",
            symbol="BTC",
            side="Buy",
            entry_price=50000.0,
            mark_price=51000.0,  # 2% profit
            update_type=PositionUpdateType.DECREASED
        )
        position_histories["BINANCE_BTC"] = [early_exit_pos]

        # Position 2: Large profit (should not trigger)
        large_profit_pos = create_position(
            venue="BINANCE",
            symbol="ETH",
            side="Buy",
            entry_price=3000.0,
            mark_price=3500.0,  # 16.7% profit
            update_type=PositionUpdateType.DECREASED
        )
        position_histories["BINANCE_ETH"] = [large_profit_pos]

        # Position 3: Loss (should not trigger)
        loss_pos = create_position(
            venue="BINANCE",
            symbol="DOGE",
            side="Buy",
            entry_price=0.1,
            mark_price=0.09,  # 10% loss
            update_type=PositionUpdateType.DECREASED
        )
        position_histories["BINANCE_DOGE"] = [loss_pos]

        patterns = self.evaluator.check_early_profit_exit(self.user_id, position_histories)

        self.assertEqual(len(patterns), 1, "Expected 1 pattern for early profit exit")

        pattern = patterns[0]
        self.assertEqual(pattern.pattern_id, "position_early_profit_exit")
        self.assertEqual(pattern.details["symbol"], "BTC")


if __name__ == "__main__":
    unittest.main()
