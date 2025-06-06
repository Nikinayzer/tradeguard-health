"""
Tests for the TrendsService class.
"""
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch, MagicMock, PropertyMock
import asyncio
import pandas as pd
import time
import logging

from src.market.trends_service import TrendsService, TrendData

logger = logging.getLogger(__name__)


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


class TestTrendsService(AsyncTestCase):
    """Test cases for TrendsService."""

    def setUp(self):
        """Set up test fixtures."""
        super().setUp()
        self.trends_service = TrendsService()

    def test_symbol_mapping(self):
        """Test symbol to search term mapping."""
        self.assertEqual(self.trends_service._get_search_term("BTC"), ("Bitcoin", True))
        self.assertEqual(self.trends_service._get_search_term("ETH"), ("Ethereum", True))
        self.assertEqual(self.trends_service._get_search_term("UNKNOWN"), ("UNKNOWN", False))

    def test_rate_limiting(self):
        """Test rate limiting functionality."""
        symbol = "BTC"

        self.assertFalse(self.trends_service._is_rate_limited(symbol))

        self.trends_service.last_request_time[symbol] = datetime.now(timezone.utc)

        self.assertTrue(self.trends_service._is_rate_limited(symbol))

        self.trends_service.last_request_time[symbol] = (
                datetime.now(timezone.utc) - self.trends_service.cooldown - timedelta(minutes=1)
        )

        self.assertFalse(self.trends_service._is_rate_limited(symbol))

    @patch('src.market.trends_service.TrendReq')
    def test_cache_management(self, mock_trend_req):
        """Test caching functionality with mocked data."""
        # Set up mock
        mock_instance = MagicMock()
        mock_trend_req.return_value = mock_instance

        # Mock interest data
        mock_interest_data = pd.DataFrame({
            'Bitcoin': [30, 40, 50, 60, 70]
        }, index=pd.date_range(start='2024-01-01', periods=5))
        mock_instance.interest_over_time.return_value = mock_interest_data

        # Mock related queries
        mock_related = {
            'Bitcoin': {
                'rising': pd.DataFrame({
                    'query': ['buy bitcoin', 'bitcoin price'],
                    'value': [100, 80]
                }),
                'top': pd.DataFrame({
                    'query': ['bitcoin news', 'bitcoin chart'],
                    'value': [90, 70]
                })
            }
        }
        mock_instance.related_queries.return_value = mock_related

        # Mock regional data
        mock_region_data = pd.DataFrame({
            'Bitcoin': [80, 60, 40]
        }, index=['US', 'UK', 'JP'])
        mock_instance.interest_by_region.return_value = mock_region_data

        # Mock build_payload to prevent URL errors
        mock_instance.build_payload = MagicMock()

        # First fetch - should get fresh data
        first_fetch = self.run_async(self.trends_service.get_coin_trends("BTC"))
        self.assertIsNotNone(first_fetch)
        self.assertEqual(first_fetch.symbol, "BTC")
        
        # Second fetch - should get cached data
        second_fetch = self.run_async(self.trends_service.get_coin_trends("BTC"))
        self.assertIsNotNone(second_fetch)
        self.assertEqual(second_fetch.symbol, "BTC")
        
        # Verify it's the same data (cached)
        self.assertEqual(first_fetch.timestamp, second_fetch.timestamp)
        
        # Test cache expiration
        self.trends_service.cache_duration = timedelta(seconds=1)  # Set short duration for testing
        self.trends_service._cache_data("BTC", first_fetch)  # Force cache with old data
        
        # Simulate time passing
        expired_time = datetime.now(timezone.utc) - timedelta(seconds=2)
        self.trends_service.cache["BTC"] = (first_fetch, expired_time)
        
        # Fetch after expiration - should get fresh data
        third_fetch = self.run_async(self.trends_service.get_coin_trends("BTC"))
        self.assertIsNotNone(third_fetch)
        self.assertEqual(third_fetch.symbol, "BTC")
        
        # Verify it's new data (not cached)
        self.assertNotEqual(first_fetch.timestamp, third_fetch.timestamp)

    @patch('src.market.trends_service.TrendReq')
    def test_get_coin_trends(self, mock_trend_req):
        """Test fetching coin trends."""
        mock_instance = MagicMock()
        mock_trend_req.return_value = mock_instance

        mock_interest_data = pd.DataFrame({
            'Bitcoin': [30, 40, 50, 60, 70]
        }, index=pd.date_range(start='2024-01-01', periods=5))
        mock_instance.interest_over_time.return_value = mock_interest_data

        mock_related = {
            'Bitcoin': {
                'rising': pd.DataFrame({
                    'query': ['buy bitcoin', 'bitcoin price'],
                    'value': [100, 80]
                }),
                'top': pd.DataFrame({
                    'query': ['bitcoin news', 'bitcoin chart'],
                    'value': [90, 70]
                })
            }
        }
        mock_instance.related_queries.return_value = mock_related

        mock_region_data = pd.DataFrame({
            'Bitcoin': [80, 60, 40]
        }, index=['US', 'UK', 'JP'])
        mock_instance.interest_by_region.return_value = mock_region_data

        mock_instance.build_payload = MagicMock()

        service = TrendsService()
        service.pytrends = mock_instance

        trend_data = self.run_async(service.get_coin_trends("BTC"))

        self.assertIsNotNone(trend_data)
        self.assertEqual(trend_data.symbol, "BTC")
        self.assertEqual(trend_data.current_interest, 70.0)
        self.assertEqual(trend_data.historical_avg, 50.0)
        self.assertAlmostEqual(trend_data.interest_change, 40.0, places=1)
        self.assertIn("buy bitcoin", trend_data.related_queries)
        self.assertIn("US", trend_data.regional_concentration)

    @patch('src.market.trends_service.TrendReq')
    def test_analyze_hype_metrics(self, mock_trend_req):
        """Test hype metrics analysis."""
        mock_instance = MagicMock()
        mock_trend_req.return_value = mock_instance

        mock_interest_data = pd.DataFrame({
            'Bitcoin': [30, 40, 50, 60, 90]  # High current interest
        }, index=pd.date_range(start='2024-01-01', periods=5))
        mock_instance.interest_over_time.return_value = mock_interest_data

        mock_related = {
            'Bitcoin': {
                'rising': pd.DataFrame({
                    'query': ['buy bitcoin now', 'bitcoin price up'],
                    'value': [100, 80]
                }),
                'top': pd.DataFrame({
                    'query': ['bitcoin growth', 'bitcoin profit'],
                    'value': [90, 70]
                })
            }
        }
        mock_instance.related_queries.return_value = mock_related

        mock_region_data = pd.DataFrame({
            'Bitcoin': [90, 60, 40]
        }, index=['US', 'UK', 'JP'])
        mock_instance.interest_by_region.return_value = mock_region_data

        mock_instance.build_payload = MagicMock()

        service = TrendsService()
        service.pytrends = mock_instance

        metrics = self.run_async(service.analyze_hype_metrics("BTC"))

        self.assertIsNotNone(metrics)
        self.assertIn('hype_score', metrics)
        self.assertIn('interest_spike', metrics)
        self.assertIn('regional_concentration', metrics)
        self.assertIn('related_query_sentiment', metrics)

        self.assertGreaterEqual(metrics['hype_score'], 0)
        self.assertLessEqual(metrics['hype_score'], 100)

        self.assertGreater(metrics['interest_spike'], 0)
        self.assertGreater(metrics['regional_concentration'], 0)
        self.assertGreater(metrics['related_query_sentiment'], 0)

    def test_real_google_trends_fetch(self):
        """Test actual data fetching from Google Trends API."""
        trend_data = self.run_async(self.trends_service.get_coin_trends("BTC"))
        print("Raw response from Google Trends:")
        if trend_data:
            print(f"Search term used: {trend_data.search_term}")
            print(f"Current interest: {trend_data.current_interest}")
            print(f"Historical avg: {trend_data.historical_avg}")
            print(f"Interest change: {trend_data.interest_change}")
            print(f"Related queries: {trend_data.related_queries}")
            print(f"Regional concentration: {trend_data.regional_concentration}")
            print(f"Timestamp: {trend_data.timestamp}")
            print(f"Timeframe: {trend_data.timeframe}")
            print(f"Is mapped: {trend_data.is_mapped}")
        else:
            logger.error("No trend data returned")
        
        # Verify we got valid data
        self.assertIsNotNone(trend_data, "Failed to fetch trend data from Google Trends")
        self.assertEqual(trend_data.symbol, "BTC")
        self.assertIsNotNone(trend_data.search_term)
        self.assertIsNotNone(trend_data.current_interest)
        self.assertIsNotNone(trend_data.historical_avg)
        self.assertIsNotNone(trend_data.interest_change)
        self.assertIsNotNone(trend_data.related_queries)
        self.assertIsNotNone(trend_data.regional_concentration)
        self.assertIsNotNone(trend_data.timestamp)
        self.assertIsNotNone(trend_data.timeframe)
        self.assertIsNotNone(trend_data.is_mapped)


if __name__ == '__main__':
    unittest.main()
