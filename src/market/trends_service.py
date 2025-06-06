"""
Google Trends Service for cryptocurrency market analysis.

This service provides functionality to fetch and analyze Google Trends data
for cryptocurrencies to detect hype patterns and market sentiment.
"""

from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
import asyncio
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
        self.cache_duration = timedelta(minutes=60)  # Cache for 1 hour

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
            # Default to 7 days if timeframe is invalid
            logger.warning(f"Invalid timeframe format: {timeframe}, defaulting to 7 days")
            start_date = end_date - timedelta(days=7)
            return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

    def _get_hourly_date_range(self) -> Tuple[str, str]:
        """Calculate date range for hourly data (last 4 hours)."""
        end_date = datetime.now()
        start_date = end_date - timedelta(hours=4)
        return start_date.strftime('%Y-%m-%d %H:%M:%S'), end_date.strftime('%Y-%m-%d %H:%M:%S')

    def _process_interest_data(self, interest_over_time: pd.DataFrame, data_column: str) -> Tuple[float, float, float]:
        """Process interest over time data to get current, average, and change values."""
        if interest_over_time.empty:
            logger.warning(f"No interest data available")
            return 0.0, 0.0, 0.0
            
        try:
            # Get the most recent non-partial data point
            recent_data = interest_over_time[~interest_over_time['isPartial']]
            if recent_data.empty:
                recent_data = interest_over_time
                
            # Get the last complete data point
            current_interest = float(recent_data[data_column].iloc[-1])
            historical_avg = float(recent_data[data_column].mean())
            interest_change = ((current_interest - historical_avg) / historical_avg) * 100 if historical_avg > 0 else 0
            return current_interest, historical_avg, interest_change
        except Exception as e:
            logger.error(f"Error processing interest data: {str(e)}")
            return 0.0, 0.0, 0.0

    def _process_related_queries(self, related_queries: Dict, data_column: str) -> Dict[str, float]:
        """Process related queries data into a dictionary."""
        queries_dict = {}
        
        # Process rising queries
        rising = related_queries.get(data_column, {}).get('rising', pd.DataFrame())
        if not rising.empty:
            for _, row in rising.iterrows():
                queries_dict[row['query']] = row['value']
        
        # Process top queries
        top = related_queries.get(data_column, {}).get('top', pd.DataFrame())
        if not top.empty:
            for _, row in top.iterrows():
                queries_dict[row['query']] = row['value']
                
        return queries_dict

    def _process_regional_data(self, interest_by_region: pd.DataFrame, data_column: str) -> Dict[str, float]:
        """Process regional interest data into a dictionary."""
        regional_dict = {}
        if not interest_by_region.empty and data_column in interest_by_region.columns:
            for region, value in interest_by_region[data_column].items():
                regional_dict[region] = float(value)
        return regional_dict

    async def _fetch_hourly_data(self, kw_list: List[str]) -> Optional[pd.DataFrame]:
        """Fetch hourly trend data."""
        try:
            start_str, end_str = self._get_hourly_date_range()
            self.pytrends.build_payload(
                kw_list=kw_list,
                timeframe=f'{start_str} {end_str}',
                geo=''
            )
            return self.pytrends.interest_over_time()
        except Exception as e:
            logger.warning(f"Failed to fetch hourly data: {str(e)}")
            return None

    async def get_coin_trends(
        self, 
        symbol: str, 
        timeframe: str = '7d',
        include_hourly: bool = True
    ) -> Optional[TrendData]:
        """Fetch and analyze Google Trends data for a cryptocurrency."""
        try:
            # Check cache first
            if cached_data := self._get_cached_data(symbol):
                logger.info(f"Using cached trend data for {symbol}")
                return cached_data

            search_term, is_mapped = self._get_search_term(symbol)
            logger.info(f"Fetching trends for {symbol} (search term: {search_term}, mapped: {is_mapped})")

            # Prepare keywords and date range
            kw_list = [symbol, search_term] if is_mapped else [symbol]
            start_str, end_str = self._get_date_range(timeframe)
            
            logger.info(f"Fetching trends with date range: {start_str} to {end_str}")
            
            # Fetch daily data
            self.pytrends.build_payload(
                kw_list=kw_list,
                timeframe=f'{start_str} {end_str}',
                geo=''
            )

            interest_over_time = self.pytrends.interest_over_time()
            if interest_over_time.empty:
                logger.warning(f"No trend data available for {symbol}")
                return None

            # Log the raw data for debugging
            logger.info(f"Raw interest over time data:\n{interest_over_time.head()}")
            
            # Process the data - use the first column (usually the symbol)
            data_column = interest_over_time.columns[0]
            
            # Get the most recent non-partial data point
            recent_data = interest_over_time[~interest_over_time['isPartial']]
            if recent_data.empty:
                recent_data = interest_over_time
                
            # Get the last complete data point
            current_interest = float(recent_data[data_column].iloc[-1])
            historical_avg = float(recent_data[data_column].mean())
            interest_change = ((current_interest - historical_avg) / historical_avg) * 100 if historical_avg > 0 else 0

            # Create and cache the trend data
            trend_data = TrendData(
                symbol=symbol,
                search_term=data_column,
                current_interest=current_interest,
                historical_avg=historical_avg,
                interest_change=interest_change,
                related_queries={},  # Empty since we're not using related queries
                regional_concentration={},  # Empty since we're not using regional data
                timestamp=datetime.now(timezone.utc),
                timeframe=timeframe,
                is_mapped=is_mapped,
                hourly_data=None  # Empty since we're not using hourly data
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
        # We want to detect:
        # - High current interest + high deviation = peak hype
        # - High current interest + low deviation = stable high interest
        # - Low current interest + high deviation = growing interest
        # - Low current interest + low deviation = low interest
        
        if current_interest > 50:
            # High current interest
            if deviation_from_avg > 0.1:  # More than 10% above average
                # Peak hype - high interest and significantly above average
                hype_score = 0.75 + (deviation_from_avg)  # 0.75-1.0 range
            else:
                # Stable high interest
                hype_score = 0.5 + (deviation_from_avg)  # 0.5-0.75 range
        else:
            # Low current interest
            if deviation_from_avg > 0.1:  # More than 10% above average
                # Growing interest - low but increasing
                hype_score = 0.25 + (deviation_from_avg)  # 0.25-0.5 range
            else:
                # Low interest
                hype_score = max(0.0, deviation_from_avg)  # 0.0-0.25 range
        
        # Adjust score based on whether the term was mapped
        # Mapped terms (e.g., "Bitcoin" instead of "BTC") indicate broader interest
        if trend_data.is_mapped:
            # For mapped terms, we're more confident in the signal
            if deviation_from_avg > 0:
                # If interest is above average for a mapped term, that's a stronger signal
                hype_score *= 1.2
            else:
                # If interest is below average for a mapped term, that's also significant
                hype_score *= 1.1
                
        # Ensure final score is between 0.0 and 1.0
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