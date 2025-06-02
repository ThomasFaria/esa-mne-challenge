import asyncio
import logging
from typing import Optional

import pycountry
import yfinance as yf

from discovery.models import OtherSources
from discovery.yahoo import YahooFetcher

logger = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Mozilla/5.0 (X11; Linux x86_64)",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:112.0) Gecko/20100101 Firefox/112.0",
]


class YahooExtractor:
    """
    A class to extract financial informations from Yahoo Finance page for MNEs.
    """

    def __init__(self, fetcher: Optional[YahooFetcher] = None):
        """
        Initialize the YahooExtractor with API endpoint
        """
        self.URL_BASE = "https://query2.finance.yahoo.com/v1/finance/search"
        self.fetcher = fetcher

    def extract_yahoo_infos(self, mne: dict) -> Optional[OtherSources]:
        """
        Extract financial information from Yahoo Finance for a given MNE.

        Args:
            mne (dict): MNE metadata.

        Returns:
            Optional[OtherSources]: Financial information or None.
        """

        # Get the Yahoo ticker symbol
        ticker = await self.get_yahoo_ticker(mne["NAME"])

        if ticker:
            ticker = yf.Ticker(ticker)
            # Get fiscal year
            year = await self.get_year(ticker)
            # Get country ISO2 code
            country = await self.get_country(ticker)
            return year, country
        else:
            logger.error(f"Yahoo Finance page not found for ticker: {ticker}")
            return None

    async def get_year(ticker: yf.Ticker) -> int:
        """
        Get the fiscal year for a given ticker symbol of a MNE.
        Args:
            ticker (yf.Ticker): Ticker object.
        Returns:
            int: Fiscal year or None if not found.
        """
        try:
            return ticker.info.get("fiscalYearEnd")
        except KeyError:
            return None

    async def get_country(ticker: yf.Ticker) -> str:
        """
        Get the country ISO2 code for a given ticker symbol of a MNE.
        Args:
            ticker (str): Ticker symbol.
        Returns:
            str: Country ISO2 code or None if not found.
        """
        tick = yf.Ticker(ticker)
        country_name = tick.info.get("country")
        if not country_name:
            return None

        return pycountry.countries.search_fuzzy(country_name)[0].alpha_2

    async def async_extract_for(self, mne: dict) -> Optional[OtherSources]:
        """
        Async wrapper to extract financial informations from Yahoo Finance page for a given MNE.

        Args:
            mne (dict): MNE metadata.

        Returns:
            Optional[List[OtherSources]]: List of sources or None.
        """
        return await self.extract_yahoo_infos(mne)

    def extract_for(self, mne: dict) -> Optional[OtherSources]:
        """
        Sync wrapper around the async Yahoo fetcher.

        Args:
            mne (dict): MNE metadata.

        Returns:
            Optional[List[OtherSources]]: List of sources or None.
        """
        return asyncio.run(self.extract_yahoo_infos(mne))
