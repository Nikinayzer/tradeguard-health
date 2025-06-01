import unittest
from unittest.mock import Mock, patch, AsyncMock
from datetime import datetime, timezone
from src.models.position_models import Position, PositionUpdateType
from src.risk.evaluators.positions_evaluator import PositionEvaluator
from src.state.state_manager import StateManager


class TestPositionVolatilityLiquidity(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.state_manager = Mock(spec=StateManager)
        self.evaluator = PositionEvaluator(self.state_manager)

        self.position_storage = Mock()
        self.state_manager.position_storage = self.position_storage

    def create_test_position(self, symbol: str, venue: str = "BYBIT", qty: float = 1.0,
                             usdt_amt: float = 1000.0, unrealized_pnl: float = 0.0) -> Position:
        """Helper method to create test positions."""
        return Position(
            venue=venue,
            symbol=symbol,
            side="Buy",
            qty=qty,
            usdt_amt=usdt_amt,
            entry_price=100.0,
            mark_price=100.0,
            unrealized_pnl=unrealized_pnl,
            cur_realized_pnl=0.0,
            cum_realized_pnl=0.0,
            leverage=1.0,
            timestamp=datetime.now(timezone.utc),
            account_name="test_account",
            user_id=1,
            update_type=PositionUpdateType.SNAPSHOT
        )

    @patch('src.market.market_data_service.MarketDataService')
    async def test_check_volatility_risk(self, mock_market_data):
        """Test volatility risk check with high volatility."""
        user_id = 1
        symbol = "BTC"
        position_key = f"BYBIT_{symbol}"

        position = self.create_test_position(symbol)
        position_histories = {position_key: [position]}

        mock_market_data.return_value.get_volatility = AsyncMock(return_value=0.75)  # 75% volatility

        patterns = await self.evaluator.check_volatility_risk(user_id, position_histories)

        self.assertEqual(len(patterns), 1)
        pattern = patterns[0]
        self.assertEqual(pattern.pattern_id, "position_high_volatility")
        self.assertEqual(pattern.position_key, position_key)
        self.assertIn("high volatility market", pattern.message)
        self.assertIn("volatility", pattern.details)
        self.assertEqual(pattern.details["volatility"], 0.75)
        self.assertEqual(pattern.details["threshold"], self.evaluator.VOLATILITY_THRESHOLD)

    @patch('src.market.market_data_service.MarketDataService')
    async def test_check_volatility_risk_below_threshold(self, mock_market_data):
        """Test volatility risk check with low volatility."""
        user_id = 1
        symbol = "BTC"
        position_key = f"BYBIT_{symbol}"

        position = self.create_test_position(symbol)
        position_histories = {position_key: [position]}

        mock_market_data.return_value.get_volatility = AsyncMock(return_value=0.25)  # 25% volatility

        patterns = await self.evaluator.check_volatility_risk(user_id, position_histories)

        self.assertEqual(len(patterns), 0)

    @patch('src.market.market_data_service.MarketDataService')
    async def test_check_liquidity_risk_high_spread(self, mock_market_data):
        """Test liquidity risk check with high spread."""
        user_id = 1
        symbol = "BTC"
        position_key = f"BYBIT_{symbol}"

        position = self.create_test_position(symbol)
        position_histories = {position_key: [position]}

        mock_market_data.return_value.get_liquidity_metrics = AsyncMock(return_value={
            "spread": 0.03,  # 3% spread
            "depth": 200000  # 200k USDT depth
        })

        patterns = await self.evaluator.check_liquidity_risk(user_id, position_histories)

        self.assertEqual(len(patterns), 1)
        pattern = patterns[0]
        self.assertEqual(pattern.pattern_id, "position_high_spread")
        self.assertEqual(pattern.position_key, position_key)
        self.assertIn("high spread market", pattern.message)
        self.assertIn("spread", pattern.details)
        self.assertEqual(pattern.details["spread"], 0.03)
        self.assertEqual(pattern.details["threshold"], self.evaluator.LIQUIDITY_THRESHOLD)

    @patch('src.market.market_data_service.MarketDataService')
    async def test_check_liquidity_risk_low_depth(self, mock_market_data):
        """Test liquidity risk check with low market depth."""
        user_id = 1
        symbol = "BTC"
        position_key = f"BYBIT_{symbol}"

        position = self.create_test_position(symbol, usdt_amt=50000.0)  # 50k USDT position
        position_histories = {position_key: [position]}

        mock_market_data.return_value.get_liquidity_metrics = AsyncMock(return_value={
            "spread": 0.01,  # 1% spread
            "depth": 50000  # 50k USDT depth
        })

        patterns = await self.evaluator.check_liquidity_risk(user_id, position_histories)

        self.assertEqual(len(patterns), 1)
        pattern = patterns[0]
        self.assertEqual(pattern.pattern_id, "position_low_liquidity")
        self.assertEqual(pattern.position_key, position_key)
        self.assertIn("low liquidity market", pattern.message)
        self.assertIn("market_depth", pattern.details)
        self.assertEqual(pattern.details["market_depth"], 50000)
        self.assertEqual(pattern.details["threshold"], self.evaluator.MIN_LIQUIDITY_DEPTH)
        self.assertEqual(pattern.details["depth_ratio"], 1.0)

    @patch('src.market.market_data_service.MarketDataService')
    async def test_check_liquidity_risk_both_issues(self, mock_market_data):
        """Test liquidity risk check with both high spread and low depth."""
        user_id = 1
        symbol = "BTC"
        position_key = f"BYBIT_{symbol}"

        position = self.create_test_position(symbol, usdt_amt=50000.0)
        position_histories = {position_key: [position]}

        mock_market_data.return_value.get_liquidity_metrics = AsyncMock(return_value={
            "spread": 0.03,  # 3% spread
            "depth": 50000  # 50k USDT depth
        })

        patterns = await self.evaluator.check_liquidity_risk(user_id, position_histories)

        self.assertEqual(len(patterns), 2)
        pattern_ids = {p.pattern_id for p in patterns}
        self.assertIn("position_high_spread", pattern_ids)
        self.assertIn("position_low_liquidity", pattern_ids)

    @patch('src.market.market_data_service.MarketDataService')
    async def test_check_liquidity_risk_no_issues(self, mock_market_data):
        """Test liquidity risk check with good market conditions."""
        user_id = 1
        symbol = "BTC"
        position_key = f"BYBIT_{symbol}"

        position = self.create_test_position(symbol)
        position_histories = {position_key: [position]}

        mock_market_data.return_value.get_liquidity_metrics = AsyncMock(return_value={
            "spread": 0.01,  # 1% spread
            "depth": 200000  # 200k USDT depth
        })

        patterns = await self.evaluator.check_liquidity_risk(user_id, position_histories)

        self.assertEqual(len(patterns), 0)

    @patch('src.market.market_data_service.MarketDataService')
    async def test_check_liquidity_risk_missing_data(self, mock_market_data):
        """Test liquidity risk check with missing market data."""
        user_id = 1
        symbol = "BTC"
        position_key = f"BYBIT_{symbol}"

        position = self.create_test_position(symbol)
        position_histories = {position_key: [position]}

        mock_market_data.return_value.get_liquidity_metrics = AsyncMock(return_value=None)

        patterns = await self.evaluator.check_liquidity_risk(user_id, position_histories)

        self.assertEqual(len(patterns), 0)


if __name__ == '__main__':
    unittest.main()
