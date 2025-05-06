import logging
import random
from typing import Dict, List

from duckduckgo_search import DDGS

from src.common.websearch.base import WebSearch
from src.discovery.models import SearchResult

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

        def get_random_headers():
            return {"User-Agent": random.choice(USER_AGENTS)}

        try:
            results = list(DDGS().text(query, max_results=self.max_results))
        except Exception:
            logger.warning("DuckDuckGo blocked initial search. Retrying with new headers.")
            try:
                results = list(DDGS(headers=get_random_headers()).text(query, max_results=self.max_results))
            except Exception:
                logger.error("Retry also failed.")
                return []

        if not results:
            logger.warning(f"No DuckDuckGo results for '{query}'")
            return []

        return [SearchResult(url=r["href"], title=r["title"], description=r["body"]) for r in results]
