import logging
from typing import Dict, List

from googlesearch import search as google_search
from pydantic import ValidationError

from src.common.websearch.base import WebSearch
from src.discovery.models import SearchResult

logger = logging.getLogger(__name__)


class GoogleSearch(WebSearch):
    def __init__(self, max_results: int = 5, region: str = None, sleep_interval: int = 0):
        self.max_results = max_results
        self.region = region
        self.sleep_interval = sleep_interval

    async def search(self, query: str) -> List[Dict]:
        logger.info(f"Searching Google for '{query}'")
        raw_results = list(
            google_search(
                query,
                num_results=self.max_results,
                proxy=None,
                advanced=True,
                sleep_interval=self.sleep_interval,
                region=self.region,
            )
        )

        if not raw_results:
            logger.warning(f"No Google results for '{query}'")
            return []

        results = []
        for r in raw_results:
            try:
                result = SearchResult(url=r.url, title=r.title, description=r.description)
                results.append(result)
            except ValidationError as e:
                logger.warning(f"Skipping invalid Google result URL: {r.url} ({e.errors()[0]['msg']})")

        return results
