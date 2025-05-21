"""
Main pipeline script for fetching annual report URLs and complementary sources
for multinational enterprises (MNEs).
"""

import asyncio
import logging
import os

from tqdm import tqdm

from src.common.data import generate_discovery_submission, load_mnes
from src.common.websearch.duckduckgo import DuckDuckGoSearch
from src.common.websearch.google import GoogleSearch
from src.discovery.fetcher import AnnualReportFetcher
from src.discovery.official_register import OfficialRegisterFetcher
from src.discovery.paths import DATA_DISCOVERY_PATH
from src.discovery.wikipedia import WikipediaFetcher
from src.discovery.yahoo import YahooFetcher

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Load MNE data from the challenge starting kit
mnes = load_mnes(DATA_DISCOVERY_PATH)

# Setup fetchers
# Fetcher for Annual Report (LLM-BASED)
fetcher = AnnualReportFetcher(
    searcher=[
        GoogleSearch(max_results=10),
        DuckDuckGoSearch(),
    ],
    model="gemma3:27b",
    base_url="https://llm.lab.sspcloud.fr/api",
    api_key=os.environ["OPENAI_API_KEY"],
)

# Fetcher for Wikipedia website
wiki = WikipediaFetcher()

# Fetcher for Yahoo Finance website
yahoo = YahooFetcher()

# Fetcher for country specific website
official_register = OfficialRegisterFetcher()


async def fetch_sources_for_mne(mne):
    """
    Fetch all sources of information for a single MNE.

    Args:
        mne (dict): A multinational enterprise entry.

    Returns:
        list: Aggregated results from all sources.
    """
    try:
        results = await asyncio.gather(
            fetcher.async_fetch_for(mne),
            yahoo.async_fetch_for(mne),
            wiki.async_fetch_for(mne),
            official_register.async_fetch_for(mne),
            return_exceptions=True,
        )

        flattened = []
        for result in results:
            if isinstance(result, Exception):
                logger.warning(f"Error fetching for {mne['name']}: {result}")
                continue
            if isinstance(result, list):
                flattened.extend(result)
            else:
                flattened.append(result)

        return flattened
    except Exception as e:
        logger.error(f"Unexpected error fetching for {mne['name']}: {e}")
        return []


async def main():
    """
    Main async function to fetch data for all MNEs.
    """
    mne_infos = []
    for mne in tqdm(mnes, desc="Fetching annual reports"):
        mne_result = await fetch_sources_for_mne(mne)
        mne_infos.append(mne_result)
    return mne_infos


if __name__ == "__main__":
    mne_infos = asyncio.run(main())
    generate_discovery_submission(mne_infos)
