import asyncio
import logging
from typing import List, Optional, Tuple

import pycountry
import tldextract
import yfinance as yf

from extractors.models import ExtractedInfo
from fetchers.models import OtherSources
from fetchers.yahoo import YahooFetcher

logger = logging.getLogger(__name__)


class YahooExtractor:
    """
    A class to extract financial informations from Yahoo Finance page for MNEs.
    """

    def __init__(self, fetcher: Optional[YahooFetcher] = None):
        """
        Initialize the YahooExtractor with the fetcher.
        """
        self.fetcher = fetcher

    async def extract_yahoo_infos(self, mne: dict, yahoo_symbol: str) -> Optional[ExtractedInfo]:
        """
        Extract financial information from Yahoo Finance for a given MNE.

        Args:
            mne (dict): MNE metadata.
            yahoo_symbol (str): Yahoo Finance ticker symbol.

        Returns:
            Optional[ExtractedInfo]: Financial information or None.
        """
        mne_name = mne.get("NAME")
        mne_id = mne.get("ID")

        if not yahoo_symbol:
            return None

        ticker = yf.Ticker(yahoo_symbol)

        try:
            tasks = [
                self.get_year(ticker),  # 0
                self.get_country(ticker),  # 1
                self.get_website(ticker),  # 2
                self.get_employees(ticker),  # 3
                self.get_turnover(ticker),  # 4
                self.get_assets(ticker),  # 5
                self.get_currency(ticker),  # 6
                self.get_activity(ticker),  # 7
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Handle exceptions in results
            infos = []
            for idx, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"Error in task {idx} for MNE {mne_name}: {result}")
                    infos.append(None)
                else:
                    infos.append(result)

            year, country, website, employees, turnover, assets, currency, activity = infos

            variables = [
                ("COUNTRY", country, "profile", "N/A"),
                ("EMPLOYEES", employees, "profile", "N/A"),
                ("TURNOVER", turnover, "financials", currency),
                ("ASSETS", assets, "balance-sheet", currency),
                ("WEBSITE", website, "profile", "N/A"),
                ("ACTIVITY", activity, "profile", "N/A"),
            ]

            results = [
                ExtractedInfo(
                    mne_id=mne_id,
                    mne_name=mne_name,
                    variable=var,
                    source_url=f"https://finance.yahoo.com/quote/{yahoo_symbol}/{page}/",
                    value=val,
                    currency=curr,
                    year=year,
                )
                for var, val, page, curr in variables
                if val is not None
            ]
            return results if results else None

        except Exception as e:
            logger.exception(f"Unhandled error extracting info for {mne_name}: {e}")
            return None

    async def get_year(self, ticker: yf.Ticker) -> int:
        """
        Get the fiscal year for a given ticker symbol of a MNE.
        Args:
            ticker (yf.Ticker): Ticker object.
        Returns:
            int: Fiscal year or None if not found.
        """
        try:
            return ticker.financials.columns[0].year
        except KeyError:
            return None

    async def get_country(self, ticker: yf.Ticker) -> str:
        """
        Get the country ISO2 code for a given ticker symbol of a MNE.
        Args:
            ticker (str): Ticker symbol.
        Returns:
            str: Country ISO2 code or None if not found.
        """
        country_name = ticker.info.get("country")
        if not country_name:
            return None

        return pycountry.countries.search_fuzzy(country_name)[0].alpha_2

    async def get_website(self, ticker: yf.Ticker) -> str:
        """
        Get the official website for a given ticker symbol of a MNE.
        Args:
            ticker (yf.Ticker): Ticker object.
        Returns:
            str: Official website URL or None if not found.
        """
        try:
            url = ticker.info.get("website")
            extracted = tldextract.extract(url)

            if extracted.domain and extracted.suffix:
                return f"{extracted.domain}.{extracted.suffix}"
            return
        except KeyError:
            return None

    async def get_employees(self, ticker: yf.Ticker) -> int:
        """
        Get the number of employees for a given ticker symbol of a MNE.
        Args:
            ticker (yf.Ticker): Ticker object.
        Returns:
            int: Number of employees or None if not found.
        """
        try:
            return ticker.info.get("fullTimeEmployees")
        except KeyError:
            return None

    async def get_turnover(self, ticker: yf.Ticker) -> int:
        """
        Get the total revenue (turnover) for a given ticker symbol of a MNE.
        Args:
            ticker (yf.Ticker): Ticker object.
        Returns:
            int: Total revenue or None if not found.
        """
        try:
            return ticker.financials.loc["Total Revenue"].iloc[0]
        except KeyError:
            return None

    async def get_assets(self, ticker: yf.Ticker) -> int:
        """
        Get the total revenue (turnover) for a given ticker symbol of a MNE.
        Args:
            ticker (yf.Ticker): Ticker object.
        Returns:
            int: Total revenue or None if not found.
        """
        try:
            return ticker.balance_sheet.loc["Total Assets"].iloc[0]
        except KeyError:
            return None

    async def get_currency(self, ticker: yf.Ticker) -> str:
        """
        Get the financial currency for a given ticker symbol of a MNE.
        Args:
            ticker (yf.Ticker): Ticker object.
        Returns:
            str: Financial currency code or None if not found.
        """
        try:
            return ticker.info.get("financialCurrency")
        except KeyError:
            return None

    async def get_activity(self, ticker: yf.Ticker) -> str:
        """
        Get the main activity of a given ticker symbol of a MNE.
        Args:
            ticker (yf.Ticker): Ticker object.
        Returns:
            str: Main activity description or None if not found.
        """
        try:
            return "\n".join(
                [
                    f"{v}: {ticker.info.get(k)}"
                    for k, v in {
                        "sector": "Sector",
                        "industry": "Industry",
                        "longBusinessSummary": "Description",
                    }.items()
                ]
            )
        except KeyError:
            return None

    async def async_extract_for(self, mne: dict) -> Tuple[Optional[List[ExtractedInfo]], Optional[List[OtherSources]]]:
        """
        Async wrapper to extract financial informations from Yahoo Finance page for a given MNE.

        Args:
            mne (dict): MNE metadata.

        Returns:
            Tuple[Optional[List[ExtractedInfo]], Optional[List[OtherSources]]]: Tuple of extracted financial information and sources, or None.
        """
        sources, yahoo_symbol = await self.fetcher.async_fetch_for(mne)
        infos = await self.extract_yahoo_infos(mne, yahoo_symbol)
        return infos, sources

    def extract_for(self, mne: dict) -> Tuple[Optional[List[ExtractedInfo]], Optional[List[OtherSources]]]:
        """
        Sync wrapper around the async Yahoo Extractor.

        Args:
            mne (dict): MNE metadata.

        Returns:
            Tuple[Optional[List[ExtractedInfo]], Optional[List[OtherSources]]]: Tuple of extracted financial information and sources, or None.
        """
        return asyncio.run(self.async_extract_for(mne))
