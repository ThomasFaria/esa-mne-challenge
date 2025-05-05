import logging
import os

from tqdm import tqdm

from src.common.data import generate_discovery_submission, load_mnes
from src.common.websearch.duckduckgo import DuckDuckGoSearch
from src.common.websearch.google import GoogleSearch
from src.discovery.fetcher import AnnualReportFetcher
from src.discovery.paths import DATA_DISCOVERY_PATH

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)


mnes = load_mnes(DATA_DISCOVERY_PATH)
fetcher = AnnualReportFetcher(
    searcher=[
        GoogleSearch(),
        DuckDuckGoSearch(),
    ],
    model="gemma3:27b",
    base_url="https://llm.lab.sspcloud.fr/ollama/v1",
    api_key=os.environ["OPENAI_API_KEY"],
)

reports = []
for mne in tqdm(mnes):
    reports.append(fetcher.fetch_for(mne))

generate_discovery_submission(reports)
