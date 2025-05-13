import logging

import pycountry
import yfinance as yf

from ..yahoo import YahooFetcher

logger = logging.getLogger(__name__)


async def get_country_for_mne(mne_name: str) -> str:
    yahoo = YahooFetcher()
    ticker = await yahoo.get_yahoo_ticker(mne_name)

    logger.info(f"Fetching country for ticker: {ticker}")
    tick = yf.Ticker(ticker)
    country_iso2 = pycountry.countries.search_fuzzy(tick.info["country"])[0].alpha_2
    logger.info(f"Country ISO2 code: {country_iso2}")
    return country_iso2
