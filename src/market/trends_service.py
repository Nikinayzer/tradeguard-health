"""
Google Trends Service for cryptocurrency market analysis.

This service provides functionality to fetch and analyze Google Trends data
for cryptocurrencies to detect hype patterns and market sentiment.
"""

from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Tuple
from dataclasses import dataclass
import pandas as pd
from pytrends.request import TrendReq
from src.utils.log_util import get_logger

logger = get_logger()


@dataclass
class TrendData:
    """Container for trend analysis data."""
    symbol: str
    search_term: str  # The actual search term used (either mapped name or symbol)
    current_interest: float
    historical_avg: float
    interest_change: float
    related_queries: Dict[str, float]
    regional_concentration: Dict[str, float]
    timestamp: datetime
    timeframe: str
    is_mapped: bool  # Whether the search term was mapped from our dictionary
    hourly_data: Optional[pd.DataFrame] = None  # Added for hourly analysis


class TrendsService:
    """
    Service for fetching and analyzing Google Trends data for cryptocurrencies.
    
    This service handles:
    - Fetching trend data for specific cryptocurrencies
    - Analyzing trend patterns and hype indicators
    - Caching results to respect rate limits
    - Normalizing search terms
    """

    def __init__(self):
        """Initialize the trends service with caching."""
        self.pytrends = TrendReq(hl='en-US', tz=360)
        self.cache: Dict[str, Tuple[TrendData, datetime]] = {}
        self.cache_duration = timedelta(minutes=60)

        self.symbol_mapping = {
            'BTC': 'Bitcoin',
            'ETH': 'Ethereum',
            'BNB': 'Binance Coin',
            'SOL': 'Solana',
            'ADA': 'Cardano',
            'XRP': 'Ripple',
            'DOT': 'Polkadot',
            'DOGE': 'Dogecoin',
            'AVAX': 'Avalanche',
            'MATIC': 'Polygon',
            'LINK': 'Chainlink',
            'UNI': 'Uniswap',
            'ATOM': 'Cosmos',
            'LTC': 'Litecoin',
            'XLM': 'Stellar',
            'ALGO': 'Algorand',
            'VET': 'VeChain',
            'MANA': 'Decentraland',
            'SAND': 'The Sandbox',
            'AXS': 'Axie Infinity'
        }

    def _get_search_term(self, symbol: str) -> Tuple[str, bool]:
        """Convert crypto symbol to appropriate search term."""
        mapped_term = self.symbol_mapping.get(symbol)
        if mapped_term:
            return mapped_term, True
        return symbol, False

    def _get_cached_data(self, symbol: str) -> Optional[TrendData]:
        """Get cached trend data if available and not expired."""
        cached_data, cache_time = self.cache.get(symbol, (None, None))
        if not cached_data or not cache_time:
            return None

        if datetime.now(timezone.utc) - cache_time > self.cache_duration:
            del self.cache[symbol]
            return None

        return cached_data

    def _get_date_range(self, timeframe: str) -> Tuple[str, str]:
        """Calculate date range for the given timeframe."""
        end_date = datetime.now()
        try:
            days = int(timeframe.replace('d', ''))
            start_date = end_date - timedelta(days=days)
            return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')
        except ValueError:
            logger.warning(f"Invalid timeframe format: {timeframe}, defaulting to 7 days")
            start_date = end_date - timedelta(days=7)
            return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

    async def get_coin_trends(
            self,
            symbol: str,
            timeframe: str = '7d',
    ) -> Optional[TrendData]:
        """Fetch and analyze Google Trends data for a cryptocurrency."""
        try:
            if cached_data := self._get_cached_data(symbol):
                logger.debug(f"Using cached trend data for {symbol}")
                return cached_data

            search_term, is_mapped = self._get_search_term(symbol)

            kw_list = [symbol, search_term] if is_mapped else [symbol]
            start_str, end_str = self._get_date_range(timeframe)

            self.pytrends.build_payload(
                kw_list=kw_list,
                timeframe=f'{start_str} {end_str}',
                geo=''
            )

            interest_over_time = self.pytrends.interest_over_time()
            if interest_over_time.empty:
                logger.warning(f"No trend data available for {symbol}")
                return None

            logger.debug(f"Raw interest over time data:\n{interest_over_time.head()}")

            # Process the data
            data_column = interest_over_time.columns[0]

            # Get the most recent non-partial data point
            recent_data = interest_over_time[~interest_over_time['isPartial']]
            if recent_data.empty:
                recent_data = interest_over_time

            # Get the last complete data point
            current_interest = float(recent_data[data_column].iloc[-1])
            historical_avg = float(recent_data[data_column].mean())
            interest_change = ((current_interest - historical_avg) / historical_avg) * 100 if historical_avg > 0 else 0

            trend_data = TrendData(
                symbol=symbol,
                search_term=data_column,
                current_interest=current_interest,
                historical_avg=historical_avg,
                interest_change=interest_change,
                related_queries={},
                regional_concentration={},
                timestamp=datetime.now(timezone.utc),
                timeframe=timeframe,
                is_mapped=is_mapped,
                hourly_data=None
            )

            self.cache[symbol] = (trend_data, datetime.now(timezone.utc))
            return trend_data

        except Exception as e:
            logger.error(f"Error fetching trends for {symbol}: {str(e)}")
            return None

    async def analyze_hype_metrics(self, symbol: str) -> Dict[str, float]:
        """Analyze hype metrics for a cryptocurrency based on interest trends."""
        trend_data = await self.get_coin_trends(symbol)
        if not trend_data:
            return {}

        # Calculate hype metrics based on:
        # 1. Current interest vs historical average
        # 2. Interest change percentage
        # 3. Whether the search term was mapped

        current_interest = trend_data.current_interest
        historical_avg = trend_data.historical_avg
        interest_change = trend_data.interest_change

        # Calculate how much current interest deviates from historical average
        # This gives us a normalized score between -1 and 1
        # -1 means current interest is 100% below average
        # 0 means current interest equals average
        # 1 means current interest is 100% above average
        deviation_from_avg = (current_interest - historical_avg) / historical_avg

        # Calculate hype score based on deviation and current level
        # - High current interest + high deviation = peak hype
        # - High current interest + low deviation = stable high interest
        # - Low current interest + high deviation = growing interest
        # - Low current interest + low deviation = low interest

        if current_interest > 50:
            if deviation_from_avg > 0.1:
                hype_score = 0.75 + deviation_from_avg
            else:
                hype_score = 0.5 + deviation_from_avg
        else:
            if deviation_from_avg > 0.1:
                hype_score = 0.25 + deviation_from_avg
            else:
                # Low interest
                hype_score = max(0.0, deviation_from_avg)

        if trend_data.is_mapped:
            if deviation_from_avg > 0:
                hype_score *= 1.2
            else:
                hype_score *= 1.1

        hype_score = min(1.0, max(0.0, hype_score))

        return {
            'hype_score': hype_score,
            'current_interest': current_interest,
            'historical_avg': historical_avg,
            'interest_change': interest_change,
            'deviation_from_avg': deviation_from_avg,
            'is_mapped': trend_data.is_mapped,
            'is_above_average': deviation_from_avg > 0
        }
