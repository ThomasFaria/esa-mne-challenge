import logging

import pycountry
import requests
import yfinance as yf

from ..wikipedia import WikipediaFetcher
from ..yahoo import YahooFetcher

logger = logging.getLogger(__name__)


async def get_country_for_mne(mne_name: str) -> str:
    yahoo = YahooFetcher()
    wiki = WikipediaFetcher()

    # Step 1: Try from Wikipedia
    try:
        wiki_name = await wiki.get_wikipedia_name(mne_name)
        if wiki_name:
            country_iso2 = await get_country_from_wikipedia(wiki_name)
            if country_iso2:
                logger.info(f"[Wikipedia] Country ISO2 code: {country_iso2}")
                return country_iso2
    except Exception as e:
        logger.warning(f"Error trying Wikipedia: {e}")

    # Step 2: Fallback to Yahoo Finance
    try:
        ticker = await yahoo.get_yahoo_ticker(mne_name)
        country_iso2 = await get_country_from_yahoo(ticker)
        if country_iso2:
            logger.info(f"[Yahoo] Country ISO2 code: {country_iso2}")
            return country_iso2
    except Exception as e:
        logger.error(f"Error trying Yahoo Finance also: {e}")

    return None


async def get_country_from_wikipedia(wiki_title: str) -> str:
    """Try to extract country ISO2 from Wikipedia/Wikidata using the page title."""
    url = "https://en.wikipedia.org/w/api.php"
    params = {"action": "query", "titles": wiki_title, "prop": "pageprops", "format": "json"}
    resp = requests.get(url, params=params).json()
    pages = resp.get("query", {}).get("pages", {})
    qid = list(pages.values())[0].get("pageprops", {}).get("wikibase_item")

    if not qid:
        return None

    # Use Wikidata to get the country (P17)
    wikidata_url = f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json"
    data = requests.get(wikidata_url).json()
    claims = data["entities"][qid]["claims"]
    country_id = claims.get("P17", [{}])[0]["mainsnak"]["datavalue"]["value"]["id"]

    # Resolve the country label
    label_url = f"https://www.wikidata.org/wiki/Special:EntityData/{country_id}.json"
    label_data = requests.get(label_url).json()
    label = label_data["entities"][country_id]["labels"]["en"]["value"]

    # Convert to ISO2
    country_iso2 = pycountry.countries.search_fuzzy(label)[0].alpha_2
    return country_iso2


async def get_country_from_yahoo(ticker: str) -> str:
    """Fallback method: extract country ISO2 from Yahoo Finance via yfinance."""
    if not ticker:
        return None

    tick = yf.Ticker(ticker)
    country_name = tick.info.get("country")
    if not country_name:
        return None

    return pycountry.countries.search_fuzzy(country_name)[0].alpha_2
