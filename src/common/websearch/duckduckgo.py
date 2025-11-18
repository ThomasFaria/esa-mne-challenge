import logging
from typing import List

from ddgs import DDGS

from common.websearch.base import WebSearch
from fetchers.models import SearchResult

logger = logging.getLogger(__name__)


class DuckDuckGoSearch(WebSearch):
    """
    Uses `duckduckgo_search` module to query DuckDuckGo for a given string and return SearchResult objects.
    """

    def __init__(self, max_results: int = 5):
        self.max_results = max_results

    async def search(self, query: str) -> List[SearchResult]:
        """
        Perform an asynchronous DuckDuckGo search.

        Args:
            query (str): The search string.
        Returns:
            List[SearchResult]: List of valid search results.
        """
        logger.debug(f"Searching DuckDuckGo for '{query}'")

        try:
            results = list(DDGS().text(query, max_results=self.max_results))
        except Exception:
            logger.warning("DuckDuckGo blocked initial search. Retrying with new headers.")
            try:
                results = list(DDGS().text(query, max_results=self.max_results))
            except Exception:
                logger.error("Retry also failed.")
                return []

        if not results:
            logger.warning(f"No DuckDuckGo results for '{query}'")
            return []

        return [SearchResult(url=r["href"], title=r["title"], description=r["body"]) for r in results]
