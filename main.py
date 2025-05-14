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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)

mnes = load_mnes(DATA_DISCOVERY_PATH)
fetcher = AnnualReportFetcher(
    searcher=[
        GoogleSearch(max_results=10),
        DuckDuckGoSearch(),
    ],
    model="gemma3:27b",
    base_url="https://llm.lab.sspcloud.fr/api",
    api_key=os.environ["OPENAI_API_KEY"],
)
wiki = WikipediaFetcher()
yahoo = YahooFetcher()
official_register = OfficialRegisterFetcher()


async def fetch_sources_for_mne(mne):
    return await asyncio.gather(
        fetcher.async_fetch_for(mne),
        yahoo.async_fetch_for(mne),
        wiki.async_fetch_for(mne),
        official_register.async_fetch_for(mne),
    )


async def main():
    mne_infos = []
    for mne in tqdm(mnes, desc="Fetching annual reports"):
        results = await fetch_sources_for_mne(mne)
        flattened = [elem for item in results for elem in (item if isinstance(item, list) else [item])]
        mne_infos.append(flattened)
    return mne_infos


mne_infos = asyncio.run(main())
generate_discovery_submission(mne_infos)
