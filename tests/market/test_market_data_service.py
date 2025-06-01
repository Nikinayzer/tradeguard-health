"""
Tests for the MarketDataService class.
"""
import unittest
from datetime import datetime
from unittest.mock import patch
import time
import asyncio

from src.market.market_data_service import MarketDataService, OrderbookData


class AsyncTestCase(unittest.TestCase):
    """Base class for async test cases."""
    
    def run_async(self, coro):
        """Run an async coroutine and return its result."""
        return asyncio.run(coro)
        
    def setUp(self):
        """Set up test fixtures."""
        super().setUp()
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        
    def tearDown(self):
        """Clean up test fixtures."""
        self.loop.close()
        super().tearDown()


class TestMarketDataService(AsyncTestCase):
    """Test cases for MarketDataService."""

    def setUp(self):
        """Set up test fixtures."""
        super().setUp()
        self.market_service = MarketDataService(testnet=True)

    def test_base_to_symbol(self):
        """Test base coin to symbol conversion."""
        self.assertEqual(self.market_service._base_to_symbol("BTC"), "BTCUSDT")
        self.assertEqual(self.market_service._base_to_symbol("ETH"), "ETHUSDT")

    def test_get_kline_data(self):
        """Test kline data retrieval."""
        mock_response = {
            "retCode": 0,
            "result": {
                "list": [
                    # [timestamp, open, high, low, close, volume, turnover]
                    ["1234567890000", "100", "110", "90", "105", "1000", "100000"],
                    ["1234567890000", "105", "115", "95", "110", "1000", "100000"],
                    ["1234567890000", "110", "120", "100", "115", "1000", "100000"]
                ]
            }
        }

        with patch.object(self.market_service.session, 'get_kline', return_value=mock_response):
            kline_data = self.market_service._get_kline_data("BTC")
            self.assertEqual(len(kline_data), 3)
            self.assertEqual(kline_data[0][4], "105")  # Check close price

    def test_calculate_volatility(self):
        """Test volatility calculation from kline data."""
        kline_data = [
            ["1234567890000", "100", "110", "90", "100", "1000", "100000"],
            ["1234567890000", "100", "110", "90", "110", "1000", "100000"],  # 10% increase
            ["1234567890000", "110", "120", "100", "99", "1000", "100000"],  # 10% decrease
        ]

        volatility = self.market_service._calculate_volatility(kline_data)
        self.assertIsInstance(volatility, float)
        self.assertGreater(volatility, 0.0)
        self.assertAlmostEqual(volatility, 1.5874, places=3)

    def test_get_liquidity_metrics(self):
        """Test liquidity metrics calculation."""
        mock_orderbook = OrderbookData(
            timestamp=datetime.now(),
            bids=[(100.0, 1.0), (99.0, 2.0)],
            asks=[(101.0, 1.0), (102.0, 2.0)]
        )

        with patch.object(self.market_service, '_get_orderbook', return_value=mock_orderbook):
            metrics = self.run_async(self.market_service.get_liquidity_metrics("BTC"))
            self.assertIn("spread", metrics)
            self.assertIn("depth", metrics)
            self.assertAlmostEqual(metrics["spread"], 0.995, places=3)
            self.assertEqual(metrics["depth"], 2.0)  # 1.0 + 1.0 within 1% of mid-price

    def test_liquidity_caching(self):
        """Test liquidity metrics caching."""
        mock_orderbook = OrderbookData(
            timestamp=datetime.now(),
            bids=[(100.0, 1.0)],
            asks=[(101.0, 1.0)]
        )

        with patch.object(self.market_service, '_get_orderbook', return_value=mock_orderbook):
            metrics1 = self.run_async(self.market_service.get_liquidity_metrics("BTC"))
            metrics2 = self.run_async(self.market_service.get_liquidity_metrics("BTC"))
            self.assertEqual(metrics1, metrics2)


class TestMarketDataServiceIntegration(AsyncTestCase):
    """Integration tests for MarketDataService with actual Bybit API calls."""

    @classmethod
    def setUpClass(cls):
        """Set up test fixtures."""
        cls.market_service = MarketDataService(testnet=True)
        cls.timeout = 100

    def test_get_actual_kline_data(self):
        """Test actual kline data retrieval from Bybit."""
        kline_data = self.market_service._get_kline_data("SOL")

        # Check that we got valid kline data
        self.assertIsInstance(kline_data, list)
        self.assertGreater(len(kline_data), 0)

        # Check structure of a single kline
        kline = kline_data[0]
        self.assertEqual(len(kline), 7)
        self.assertIsInstance(float(kline[4]), float)

    def test_get_actual_volatility(self):
        """Test actual volatility calculation from Bybit data."""
        volatility = self.run_async(self.market_service.get_volatility("SOL"))

        # Check that we got a valid volatility value
        self.assertIsInstance(volatility, float)
        self.assertGreater(volatility, 0.0)

    def test_get_actual_orderbook(self):
        """Test actual orderbook data retrieval from Bybit."""
        orderbook = self.market_service._get_orderbook("SOL")

        # Check orderbook structure
        self.assertIsNotNone(orderbook)
        self.assertIsInstance(orderbook, OrderbookData)
        self.assertIsInstance(orderbook.timestamp, datetime)

        # Check bids and asks
        self.assertIsInstance(orderbook.bids, list)
        self.assertIsInstance(orderbook.asks, list)

        # Check that we have some orders
        self.assertGreater(len(orderbook.bids), 0)
        self.assertGreater(len(orderbook.asks), 0)

        # Check order format
        for bid in orderbook.bids:
            self.assertEqual(len(bid), 2)
            self.assertIsInstance(bid[0], float)  # price
            self.assertIsInstance(bid[1], float)  # quantity

        for ask in orderbook.asks:
            self.assertEqual(len(ask), 2)
            self.assertIsInstance(ask[0], float)
            self.assertIsInstance(ask[1], float)

    def test_get_actual_liquidity_metrics(self):
        """Test actual liquidity metrics calculation from Bybit data."""
        start_time = time.time()
        metrics = self.run_async(self.market_service.get_liquidity_metrics("BTC"))
        elapsed_time = time.time() - start_time

        self.assertLess(elapsed_time, self.timeout, "Liquidity metrics request took too long")

        self.assertIsInstance(metrics, dict)
        self.assertIn("spread", metrics)
        self.assertIn("depth", metrics)

        self.assertIsInstance(metrics["spread"], float)
        self.assertIsInstance(metrics["depth"], float)
        self.assertGreater(metrics["spread"], 0.0)
        self.assertGreater(metrics["depth"], 0.0)


if __name__ == '__main__':
    unittest.main()
