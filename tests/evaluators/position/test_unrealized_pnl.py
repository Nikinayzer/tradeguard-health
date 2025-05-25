import unittest
from datetime import datetime, timezone
from typing import Dict, List

from src.models import Position
from src.models.position_models import PositionUpdateType
from src.models.risk_models import RiskCategory
from src.risk.evaluators.positions_evaluator import PositionEvaluator
from src.state.state_manager import StateManager


def create_position(
        venue: str,
        symbol: str,
        side: str,
        entry_price: float,
        mark_price: float,
        unrealized_pnl: float,
        usdt_amt: float = 1000.0,
        update_type: str = PositionUpdateType.SNAPSHOT,
        user_id: int = 12345
) -> Position:
    """Create a position object with common default values"""
    return Position(
        venue=venue,
        symbol=symbol,
        side=side,
        qty=1.0,
        usdt_amt=usdt_amt,
        entry_price=entry_price,
        mark_price=mark_price,
        liquidation_price=None,
        unrealized_pnl=unrealized_pnl,
        cur_realized_pnl=0.0,
        cum_realized_pnl=0.0,
        leverage=1.0,
        timestamp=datetime.now(timezone.utc),
        account_name="test_account",
        user_id=user_id,
        update_type=update_type
    )


class TestUnrealizedPnl(unittest.TestCase):
    """Test case for the check_unrealized_pnl function in PositionEvaluator"""

    def setUp(self):
        """Set up the test environment before each test"""
        self.state_manager = StateManager()
        self.evaluator = PositionEvaluator(self.state_manager)
        self.user_id = 12345

    def test_no_positions(self):
        """Test that no patterns are created when there are no positions"""
        position_histories = {}

        patterns = self.evaluator.check_unrealized_pnl(self.user_id, position_histories)
        self.assertEqual(len(patterns), 0, "Expected no patterns when no positions exist")

    def test_positive_unrealized_pnl(self):
        """Test that no patterns are created for positions with positive PnL"""
        # Create position with positive PnL
        binance_btc_pos = create_position(
            venue="BINANCE",
            symbol="BTC",
            side="Buy",
            entry_price=50000.0,
            mark_price=51000.0,
            unrealized_pnl=1000.0  # Positive PnL
        )

        position_histories = {
            'BINANCE_BTC': [binance_btc_pos]
        }

        patterns = self.evaluator.check_unrealized_pnl(self.user_id, position_histories)
        self.assertEqual(len(patterns), 0, "Expected no patterns for positive PnL")

    def test_small_negative_unrealized_pnl(self):
        """Test that no patterns are created for positions with small negative PnL"""
        # Create position with small negative PnL (-5%)
        binance_eth_pos = create_position(
            venue="BINANCE",
            symbol="ETH",
            side="Buy",
            entry_price=3000.0,
            mark_price=2850.0,
            unrealized_pnl=-50.0,  # -5% of usdt_amt (1000)
        )

        position_histories = {
            'BINANCE_ETH': [binance_eth_pos]
        }

        patterns = self.evaluator.check_unrealized_pnl(self.user_id, position_histories)
        self.assertEqual(len(patterns), 0, "Expected no patterns for small negative PnL")

    def test_significant_negative_unrealized_pnl(self):
        """Test detection of positions with significant negative PnL"""
        # Create position with significant negative PnL (-15%)
        binance_sol_pos = create_position(
            venue="BINANCE",
            symbol="SOL",
            side="Buy",
            entry_price=100.0,
            mark_price=85.0,
            unrealized_pnl=-150.0,  # -15% of usdt_amt (1000)
        )

        position_histories = {
            'BINANCE_SOL': [binance_sol_pos]
        }

        patterns = self.evaluator.check_unrealized_pnl(self.user_id, position_histories)

        # Verify we got 1 pattern
        self.assertEqual(len(patterns), 1, "Expected 1 pattern for significant negative PnL")

        # Verify pattern details
        pattern = patterns[0]
        self.assertEqual(pattern.pattern_id, "position_unrealized_pnl_threshold")
        self.assertEqual(pattern.details["symbol"], "SOL")
        self.assertEqual(pattern.details["venue"], "BINANCE")
        self.assertAlmostEqual(pattern.details["pnl_percentage"], -15.0, places=1)

    def test_zero_position_size(self):
        """Test that no patterns are created for positions with zero size"""
        zero_size_pos = create_position(
            venue="BINANCE",
            symbol="AVAX",
            side="Buy",
            entry_price=20.0,
            mark_price=18.0,
            unrealized_pnl=-200.0,
            usdt_amt=0.0  # Zero position size
        )

        position_histories = {
            'BINANCE_AVAX': [zero_size_pos]
        }

        patterns = self.evaluator.check_unrealized_pnl(self.user_id, position_histories)
        self.assertEqual(len(patterns), 0, "Expected no patterns for zero size position")

    def test_multiple_positions(self):
        """Test with multiple positions, only some of which have significant negative PnL"""
        position_histories = {}

        # Position 1: Significant negative PnL (should trigger pattern)
        big_loss_pos = create_position(
            venue="BINANCE",
            symbol="BTC",
            side="Buy",
            entry_price=50000.0,
            mark_price=40000.0,
            unrealized_pnl=-200.0,  # -20% of usdt_amt (1000)
        )
        position_histories["BINANCE_BTC"] = [big_loss_pos]

        # Position 2: Small negative PnL (should not trigger)
        small_loss_pos = create_position(
            venue="BINANCE",
            symbol="ETH",
            side="Buy",
            entry_price=3000.0,
            mark_price=2850.0,
            unrealized_pnl=-50.0,  # -5% of usdt_amt (1000)
        )
        position_histories["BINANCE_ETH"] = [small_loss_pos]

        # Position 3: Positive PnL (should not trigger)
        profit_pos = create_position(
            venue="BINANCE",
            symbol="DOGE",
            side="Buy",
            entry_price=0.1,
            mark_price=0.11,
            unrealized_pnl=100.0,  # 10% of usdt_amt (1000)
        )
        position_histories["BINANCE_DOGE"] = [profit_pos]

        patterns = self.evaluator.check_unrealized_pnl(self.user_id, position_histories)

        self.assertEqual(len(patterns), 1, "Expected 1 pattern for significant negative PnL")

        pattern = patterns[0]
        self.assertEqual(pattern.pattern_id, "position_unrealized_pnl_threshold")
        self.assertEqual(pattern.details["symbol"], "BTC")
        self.assertAlmostEqual(pattern.details["pnl_percentage"], -20.0, places=1)

    def test_uses_most_recent_position(self):
        """Test that check_unrealized_pnl uses the most recent position state"""
        # Create a history with multiple position states, newest first
        # The newest should have a significant loss, while older ones don't
        
        # Newest position (significant loss)
        current_pos = create_position(
            venue="BINANCE",
            symbol="BTC",
            side="Buy",
            entry_price=50000.0,
            mark_price=40000.0,
            unrealized_pnl=-200.0,  # -20% of usdt_amt (1000)
            update_type=PositionUpdateType.SNAPSHOT,
        )
        
        # Older position (small loss)
        older_pos = create_position(
            venue="BINANCE",
            symbol="BTC",
            side="Buy",
            entry_price=50000.0,
            mark_price=48000.0,
            unrealized_pnl=-40.0,  # -4% of usdt_amt (1000)
            update_type=PositionUpdateType.SNAPSHOT,
        )
        
        # Oldest position (profit)
        oldest_pos = create_position(
            venue="BINANCE",
            symbol="BTC",
            side="Buy",
            entry_price=50000.0, 
            mark_price=52000.0,
            unrealized_pnl=40.0,  # 4% of usdt_amt (1000)
            update_type=PositionUpdateType.SNAPSHOT,
        )
        
        position_histories = {
            'BINANCE_BTC': [current_pos, older_pos, oldest_pos]  # Newest first
        }
        
        patterns = self.evaluator.check_unrealized_pnl(self.user_id, position_histories)
        
        # Should get a pattern for the current position's significant loss
        self.assertEqual(len(patterns), 1, "Expected 1 pattern based on most recent position")
        self.assertAlmostEqual(patterns[0].details["pnl_percentage"], -20.0, places=1)


if __name__ == "__main__":
    unittest.main() 