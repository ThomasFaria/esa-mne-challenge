import asyncio
import json
import logging
import os
import random
from typing import List, Optional

import requests
import yfinance as yf

from discovery.models import OtherSources
from discovery.utils import clean_mne_name

logger = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Mozilla/5.0 (X11; Linux x86_64)",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:112.0) Gecko/20100101 Firefox/112.0",
]


class YahooFetcher:
    """
    A class to fetch Yahoo Finance pages for MNEs.
    """

    def __init__(self):
        """
        Initialize the YahooFetcher with API endpoint, cache path,
        and a list of supported exchanges for filtering ticker results.
        """
        self.URL_BASE = "https://query2.finance.yahoo.com/v1/finance/search"
        self.CACHE_PATH = "cache/tickers_cache.json"
        self.EXCHANGE_LIST = [
            "CXE",
            "NYQ",
            "FRA",
            "PAR",
            "GER",
            "VIE",
            "SHZ",
            "BUD",
            "OQX",
            "SHH",
            "OEM",
            "IOB",
            "CPH",
            "HEL",
            "MCE",
        ]
        self.ticker_cache = self._load_cache(self.CACHE_PATH)

    def _load_cache(self, cache_path: str) -> dict:
        """
        Load the cached Yahoo tickers from a local JSON file.

        Args:
            cache_path (str): Path to the JSON cache.

        Returns:
            dict: Dictionary mapping MNE names to Yahoo tickers.
        """
        if not os.path.exists("cache"):
            os.makedirs("cache")
        if os.path.exists(cache_path):
            try:
                with open(cache_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Failed to read cache: {e}")
                return {}
        return {}

    def _save_cache(self, cache_path: str):
        """
        Save the current ticker cache to disk in JSON format.

        Args:
            cache_path (str): Path to the output file.
        """
        try:
            with open(cache_path, "w", encoding="utf-8") as f:
                sorted_cache = dict(sorted(self.ticker_cache.items()))
                json.dump(sorted_cache, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Failed to write cache: {e}")

    async def get_yahoo_ticker(self, mne_name: str) -> str:
        """
        Retrieve the Yahoo Finance ticker for a given MNE name.

        Args:
            mne_name (str): Full name of the MNE.

        Returns:
            Optional[str]: Ticker symbol if found, else None.
        """
        if mne_name in self.ticker_cache:
            return self.ticker_cache[mne_name]

        # Clean the MNE name
        mne_name_cleaned = clean_mne_name(mne_name)
        if any(
            keyword in mne_name_cleaned
            for keyword in [
                "GEELY",
                "WIENERBERGER",
                "STRABAG",
                "FLETCHER",
                "VOESTALPINE",
                "HBIS",
                "NESTLE",
                "HENKEL",
                "CANON INCORPORATED",
                "ANDRITZ",
                "JBS",
            ]
        ):
            mne_name_cleaned = mne_name_cleaned.split(" ")[0]
        if "SWIRE" in mne_name_cleaned:
            mne_name_cleaned = mne_name_cleaned.split(" ")[1]
        if "MAERSK" in mne_name_cleaned:
            mne_name_cleaned = mne_name_cleaned.split(" ")[-1]
        if "MOL HUNGARIAN" in mne_name_cleaned:
            mne_name_cleaned = " ".join(mne_name_cleaned.split(" ")[:2])
        if any(
            keyword in mne_name_cleaned
            for keyword in [
                "ASSECO",
                "GI GROUP",
            ]
        ):
            mne_name_cleaned = f"{mne_name_cleaned} POLAND"

        # Randomly select a user agent for the request
        headers = {"User-Agent": random.choice(USER_AGENTS)}
        params = {
            "q": mne_name_cleaned,
            "quotes_count": 10,
        }
        response = requests.get(self.URL_BASE, headers=headers, params=params)
        # Check if the request was successful
        if response.status_code != 200:
            logger.error(f"Failed to fetch Yahoo ticker for {mne_name}: {response.status_code}")
            return None

        try:
            ticker = [
                r
                for r in response.json()["quotes"]
                if r["quoteType"] == "EQUITY" and r["exchange"] in self.EXCHANGE_LIST
            ][0]["symbol"]
            self.ticker_cache[mne_name] = ticker
            self._save_cache(self.CACHE_PATH)
            return ticker
        except (IndexError, KeyError):
            logger.error(
                f"Could not find a valid Yahoo Finance ticker for '{mne_name}'. Ensure the company is publicly traded and has a profile on Yahoo Finance."
            )
            return None

    async def fetch_yahoo_page(self, mne: dict) -> Optional[List[OtherSources]]:
        """
        Retrieve profile and financials pages for a company via Yahoo Finance.

        Args:
            mne (dict): Dictionary with at least keys "ID" and "NAME".

        Returns:
            Optional[List[OtherSources]]: List of two OtherSources (profile and financials), or None.
        """
        # Get the Yahoo ticker symbol
        ticker = await self.get_yahoo_ticker(mne["NAME"])

        if ticker:
            # Make sure the ticker is valid by making a request to Yahoo
            status = requests.head(
                f"https://finance.yahoo.com/quote/{ticker}/profile/", headers={"User-Agent": "Mozilla/5.0"}
            ).status_code
            # Get the most recent year of the financials
            year = yf.Ticker(ticker).financials.columns[0].year

            if status == 200:
                return [
                    OtherSources(
                        mne_id=mne["ID"],
                        mne_name=mne["NAME"],
                        source_name="Yahoo",
                        url=f"https://finance.yahoo.com/quote/{ticker}/profile/",
                        year=year,
                    ),
                    OtherSources(
                        mne_id=mne["ID"],
                        mne_name=mne["NAME"],
                        source_name="Yahoo",
                        url=f"https://finance.yahoo.com/quote/{ticker}/financials/",
                        year=year,
                    ),
                ]
            else:
                logger.error(f"Yahoo Finance page not found for ticker: {ticker}")
                return None

    async def async_fetch_for(self, mne: dict) -> Optional[OtherSources]:
        """
        Async wrapper to fetch Yahoo Finance pages for a given MNE.

        Args:
            mne (dict): MNE metadata.

        Returns:
            Optional[List[OtherSources]]: List of sources or None.
        """
        return await self.fetch_yahoo_page(mne)

    def fetch_for(self, mne: dict) -> Optional[OtherSources]:
        """
        Sync wrapper around the async Yahoo fetcher.

        Args:
            mne (dict): MNE metadata.

        Returns:
            Optional[List[OtherSources]]: List of sources or None.
        """
        return asyncio.run(self.fetch_yahoo_page(mne))
