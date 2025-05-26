import logging
import random
from typing import Dict, List

from duckduckgo_search import DDGS

from common.websearch.base import WebSearch
from discovery.models import SearchResult

USER_AGENTS = [
    "Mozilla/5.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Mozilla/5.0 (X11; Linux x86_64)",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:112.0) Gecko/20100101 Firefox/112.0",
]

logger = logging.getLogger(__name__)


class DuckDuckGoSearch(WebSearch):
    def __init__(self, max_results: int = 5):
        self.max_results = max_results

    async def search(self, query: str) -> List[Dict]:
        logger.info(f"Searching DuckDuckGo for '{query}'")

        # Randomly select a user agent for the request
        ua1 = random.choice(USER_AGENTS)
        try:
            results = list(DDGS(headers={"User-Agent": ua1}).text(query, max_results=self.max_results))
        except Exception:
            logger.warning("DuckDuckGo blocked initial search. Retrying with new headers.")
            try:
                # Retry with a different user agent
                ua2 = random.choice([ua for ua in USER_AGENTS if ua != ua1])
                results = list(DDGS(headers={"User-Agent": ua2}).text(query, max_results=self.max_results))
            except Exception:
                logger.error("Retry also failed.")
                return []

        if not results:
            logger.warning(f"No DuckDuckGo results for '{query}'")
            return []

        return [SearchResult(url=r["href"], title=r["title"], description=r["body"]) for r in results]
