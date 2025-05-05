import logging
from typing import Dict, List

from googlesearch import search as google_search

from src.common.websearch.base import WebSearch
from src.discovery.models import SearchResult

logger = logging.getLogger(__name__)


class GoogleSearch(WebSearch):
    def __init__(self, max_results: int = 5):
        self.max_results = max_results

    async def search(self, query: str) -> List[Dict]:
        logger.info(f"Searching Google for '{query}'")
        results = list(google_search(query, num_results=self.max_results, proxy=None, advanced=True, sleep_interval=0))
        if not results:
            logger.warning(f"No Google results for '{query}'")
        return [SearchResult(url=r.url, title=r.title, description=r.description) for r in results]
