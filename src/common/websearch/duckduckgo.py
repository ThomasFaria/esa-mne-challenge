import logging
from typing import Dict, List

from duckduckgo_search import DDGS

from src.common.websearch.base import WebSearch
from src.discovery.models import SearchResult

logger = logging.getLogger(__name__)


class DuckDuckGoSearch(WebSearch):
    def __init__(self, max_results: int = 5):
        self.max_results = max_results

    async def search(self, query: str) -> List[Dict]:
        logger.info(f"Searching DuckDuckGo for '{query}'")
        results = list(DDGS().text(query, max_results=self.max_results))
        if not results:
            logger.warning(f"No DuckDuckGo results for '{query}'")
        return [SearchResult(url=r["href"], title=r["title"], description=r["body"]) for r in results]
