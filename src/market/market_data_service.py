"""
Market Data Service

Provides market data from Bybit including volatility, orderbook, and liquidity metrics.
"""
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import time
import numpy as np
from pybit.unified_trading import HTTP
from src.utils.log_util import get_logger
from functools import wraps
import asyncio
from threading import Lock

logger = get_logger()


class KlineInterval(str, Enum):
    MIN_1 = "1"
    MIN_3 = "3"
    MIN_5 = "5"
    MIN_15 = "15"
    MIN_30 = "30"
    HOUR_1 = "60"
    HOUR_2 = "120"
    HOUR_4 = "240"
    HOUR_6 = "360"
    HOUR_12 = "720"
    DAY = "D"
    WEEK = "W"
    MONTH = "M"


def with_timeout(timeout_seconds):
    """Decorator to add timeout to async functions."""

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await asyncio.wait_for(func(*args, **kwargs), timeout=timeout_seconds)
            except asyncio.TimeoutError:
                logger.error(f"Request timed out after {timeout_seconds} seconds")
                return {"retCode": -1, "retMsg": "Request timed out"}
            except Exception as e:
                logger.error(f"Request failed: {str(e)}")
                return {"retCode": -1, "retMsg": str(e)}

        return wrapper

    return decorator


@dataclass
class OrderbookData:
    """Orderbook data structure."""
    timestamp: datetime
    bids: List[Tuple[float, float]]  # (price, size)
    asks: List[Tuple[float, float]]  # (price, size)


class MarketDataService:
    """Service for fetching market data from Bybit."""

    def __init__(self, testnet: bool = False):
        """
        Initialize the market data service.
        
        Args:
            testnet: (default: False)
        """
        self.session = HTTP(testnet=testnet)
        self._volatility_cache = {}  # base_coin -> (value, timestamp)
        self._orderbook_cache = {}  # symbol -> (orderbook, timestamp)
        self._liquidity_cache = {}  # symbol -> (metrics, timestamp)
        self._cache_ttl = {
            'volatility': 300,
            'orderbook': 5,
            'liquidity': 60
        }
        self._api_timeout = 10
        self._max_retries = 3
        self._base_retry_delay = 1
        self._max_retry_delay = 10
        self._cache_lock = Lock()
        logger.info(f"MarketDataService initialized with testnet={testnet}")

    def _base_to_symbol(self, base_coin: str) -> str:
        """Convert base coin to Bybit symbol."""
        return f"{base_coin}USDT"

    def _get_kline_data(self, symbol: str, interval: KlineInterval = KlineInterval.DAY, limit: int = 100) -> List[List[str]]:
        """
        Get kline (candlestick) data from Bybit.
        
        Args:
            symbol: Trading symbol (without base)
            interval: Kline interval (default: "1d" for daily)
            limit: Number of candles to fetch (default: 30)
            
        Returns:
            List of kline data points
        """
        try:
            formatted_symbol = symbol if "USDT" in symbol else self._base_to_symbol(symbol)
            logger.info(f"Fetching kline data for {formatted_symbol} with interval {interval}")

            response = self.session.get_kline(
                category="linear",
                symbol=formatted_symbol,
                interval=interval.value,
                limit=limit,
            )

            if response["retCode"] == 0 and "result" in response and "list" in response["result"]:
                kline_data = response["result"]["list"]
                logger.info(f"Successfully retrieved {len(kline_data)} kline data points for {formatted_symbol}")
                return kline_data
            else:
                error_msg = response.get("retMsg", "Unknown error")
                logger.error(f"Failed to get kline data for {formatted_symbol}: {error_msg}")
                return []

        except Exception as e:
            logger.error(f"Error getting kline data for {symbol}: {str(e)}")
            return []

    def _calculate_volatility(self, kline_data: List[List], interval: KlineInterval = KlineInterval.DAY) -> float:
        """
        Calculate volatility from kline data.
        
        Args:
            kline_data: List of kline data points
            interval: Time interval for the kline data
            
        Returns:
            Annualized volatility as a decimal (e.g., 0.25 for 25%)
        """
        if not kline_data:
            logger.warning("No kline data provided for volatility calculation")
            return 0.0
            
        try:
            # Extract close prices
            closes = [float(k[4]) for k in kline_data]

            returns = np.diff(closes) / closes[:-1]
            std_dev = np.std(returns)
            interval_minutes = self._get_interval_minutes(interval)
            
            # Annualize volatility
            # Formula: std_dev * sqrt(annualization_factor)
            # annualization_factor = (minutes_in_year / interval_minutes)
            minutes_in_year = 252 * 24 * 60 # 252 trading days
            annualization_factor = minutes_in_year / interval_minutes
            annualized_vol = std_dev * np.sqrt(annualization_factor)
            
            logger.debug(f"Calculated volatility: {annualized_vol:.4f} for interval {interval}")
            return float(annualized_vol)
            
        except Exception as e:
            logger.error(f"Error calculating volatility: {str(e)}")
            return 0.0

    def _get_interval_minutes(self, interval: KlineInterval) -> int:
        """
        Convert interval to minutes.
        
        Args:
            interval: KlineInterval enum value
            
        Returns:
            Number of minutes in the interval
        """
        if interval == KlineInterval.MIN_1:
            return 1
        elif interval == KlineInterval.MIN_3:
            return 3
        elif interval == KlineInterval.MIN_5:
            return 5
        elif interval == KlineInterval.MIN_15:
            return 15
        elif interval == KlineInterval.MIN_30:
            return 30
        elif interval == KlineInterval.HOUR_1:
            return 60
        elif interval == KlineInterval.HOUR_2:
            return 120
        elif interval == KlineInterval.HOUR_4:
            return 240
        elif interval == KlineInterval.HOUR_6:
            return 360
        elif interval == KlineInterval.HOUR_12:
            return 720
        elif interval == KlineInterval.DAY:
            return 1440
        elif interval == KlineInterval.WEEK:
            return 10080
        elif interval == KlineInterval.MONTH:
            return 43200
        else:
            logger.warning(f"Unknown interval {interval}, defaulting to 1 day")
            return 1440

    @with_timeout(10)  # 10 second timeout
    async def get_volatility(self, base_coin: str, interval: KlineInterval = KlineInterval.DAY) -> Optional[float]:
        """
        Get historical volatility for a base coin.
        
        Args:
            base_coin: Base coin symbol (e.g., 'BTC')
            interval: Time interval for volatility calculation
            
        Returns:
            Annualized volatility as a decimal (e.g., 0.25 for 25%) or None if timeout/error
        """
        cache_key = f"{base_coin}_{interval}"
        with self._cache_lock:
            if cache_key in self._volatility_cache:
                value, timestamp = self._volatility_cache[cache_key]
                if datetime.now().timestamp() - timestamp < self._cache_ttl['volatility']:
                    logger.debug(f"Using cached volatility for {base_coin} at interval {interval}")
                    return value
                
        try:
            kline_data = await asyncio.to_thread(self._get_kline_data, base_coin, interval)
            volatility = await asyncio.to_thread(self._calculate_volatility, kline_data, interval)

            with self._cache_lock:
                self._volatility_cache[cache_key] = (volatility, datetime.now().timestamp())
            
            return volatility
            
        except asyncio.TimeoutError:
            logger.error(f"Timeout getting volatility for {base_coin}")
            return None
        except Exception as e:
            logger.error(f"Error getting volatility for {base_coin}: {str(e)}")
            return None

    def get_liquidity_metrics(self, base_coin: str) -> Dict[str, float] | None:
        """
        Get liquidity metrics for a base coin.
        
        Args:
            base_coin: Base coin (e.g., "BTC")
            
        Returns:
            Dictionary with spread and depth metrics, or None if data cannot be retrieved
        """
        try:
            cache_key = base_coin.upper()
            if cache_key in self._liquidity_cache:
                metrics, timestamp = self._liquidity_cache[cache_key]
                if time.time() - timestamp < self._cache_ttl['liquidity']:
                    return metrics

            orderbook = self._get_orderbook(base_coin)
            if not orderbook:
                return None

            metrics = self._calculate_liquidity(orderbook)
            self._liquidity_cache[cache_key] = (metrics, time.time())
            return metrics

        except Exception as e:
            logger.error(f"Error getting liquidity metrics for {base_coin}: {str(e)}")
            return None

    @with_timeout(10)
    async def get_liquidity_metrics(self, base_coin: str) -> Optional[Dict[str, float]]:
        """
        Get liquidity metrics for a base coin.
        
        Args:
            base_coin: Base coin (e.g., "BTC")
            
        Returns:
            Dictionary with spread and depth metrics or None if timeout/error
        """
        try:
            cache_key = base_coin.upper()
            with self._cache_lock:
                if cache_key in self._liquidity_cache:
                    metrics, timestamp = self._liquidity_cache[cache_key]
                    if time.time() - timestamp < self._cache_ttl['liquidity']:
                        return metrics

            orderbook = await asyncio.to_thread(self._get_orderbook, base_coin)
            if not orderbook:
                return None

            metrics = await asyncio.to_thread(self._calculate_liquidity, orderbook)
            with self._cache_lock:
                self._liquidity_cache[cache_key] = (metrics, time.time())
            return metrics

        except asyncio.TimeoutError:
            logger.error(f"Timeout getting liquidity metrics for {base_coin}")
            return None
        except Exception as e:
            logger.error(f"Error getting liquidity metrics for {base_coin}: {str(e)}")
            return None

    def _get_orderbook(self, base_coin: str) -> Optional[OrderbookData]:
        """
        Get orderbook data for a base coin.
        
        Args:
            base_coin: Base coin (e.g., "BTC")
            
        Returns:
            OrderbookData object or None if failed
        """
        try:
            symbol = self._base_to_symbol(base_coin)
            if symbol in self._orderbook_cache:
                orderbook, timestamp = self._orderbook_cache[symbol]
                if time.time() - timestamp < self._cache_ttl['orderbook']:
                    return orderbook

            response = self.session.get_orderbook(
                category="linear",
                symbol=symbol,
                timeout=self._api_timeout
            )

            if response["retCode"] == 0 and "result" in response:
                result = response["result"]
                bids = [(float(price), float(size)) for price, size in result["b"]]
                asks = [(float(price), float(size)) for price, size in result["a"]]

                orderbook = OrderbookData(
                    timestamp=datetime.now(),
                    bids=bids,
                    asks=asks
                )

                self._orderbook_cache[symbol] = (orderbook, time.time())
                return orderbook

            logger.warning(f"Failed to get orderbook for {symbol}: {response}")
            return None

        except Exception as e:
            logger.error(f"Error getting orderbook for {base_coin}: {str(e)}")
            return None

    def _calculate_liquidity(self, orderbook: OrderbookData) -> Dict[str, float] | None:
        """
        Calculate liquidity metrics from orderbook data.

        Returns:
            Dictionary with spread (%) and depth (USDT volume within ±1% of mid),
            or None if calculation fails
        """
        try:
            if not orderbook.bids or not orderbook.asks:
                return None

            best_bid = orderbook.bids[0][0]
            best_ask = orderbook.asks[0][0]
            mid_price = (best_bid + best_ask) / 2
            spread = ((best_ask - best_bid) / mid_price) * 100

            price_range = mid_price * 0.01
            depth = 0.0

            # Bids 1% below mid_price
            for price, size in orderbook.bids:
                if mid_price - price_range <= price <= mid_price:
                    depth += size * price  # Convert to USDT by multiplying by price

            # Asks 1% above mid_price
            for price, size in orderbook.asks:
                if mid_price <= price <= mid_price + price_range:
                    depth += size * price  # Convert to USDT by multiplying by price

            return {"spread": spread, "depth": depth}

        except Exception as e:
            logger.error(f"Error calculating liquidity metrics: {str(e)}")
            return None
