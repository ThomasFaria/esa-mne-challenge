import asyncio
import json
import logging
import os
import re
from typing import List, Optional, Union

import aiohttp
import requests
from langfuse import Langfuse
from langfuse.decorators import observe
from langfuse.openai import AsyncOpenAI
from tqdm.asyncio import tqdm

from common.websearch.base import WebSearch

from .models import AnnualReport

logger = logging.getLogger(__name__)


class AnnualReportFetcher:
    """
    Fetches the most recent annual report PDF URLs for multinational enterprises (MNEs),
    using a combination of search engines, LLM parsing, and URL validation.
    """

    def __init__(
        self,
        searcher: Union[WebSearch, List[WebSearch]],
        llm_client: AsyncOpenAI,
        model: str = "gemma3:27b",
    ):
        self.client = llm_client
        self.model = model
        self.prompt = Langfuse().get_prompt("annual-report-extractor", label="production")
        self.CACHE_PATH = "cache/reports_cache.json"
        self.reports_cache = self._load_cache(self.CACHE_PATH)

        if isinstance(searcher, list):
            self.searchers = searcher
        else:
            self.searchers = [searcher]

    def _load_cache(self, cache_path: str) -> dict:
        """
        Load previously fetched annual reports from a local JSON cache if it exists, otherwise, it creates one.

        Args:
            cache_path (str): Path to the JSON cache file.

        Returns:
            dict: A dictionary mapping MNE names to [year, pdf_url].
        """
        if not os.path.exists("cache"):
            os.makedirs("cache")
        if os.path.exists(cache_path):
            try:
                with open(cache_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Failed to read cache: {e}")
                return {}
        return {}

    def _save_cache(self, cache_path: str):
        """
        Save the current report cache to a local file.

        Args:
            cache_path (str): Path to the JSON cache file.
        """
        try:
            with open(cache_path, "w", encoding="utf-8") as f:
                sorted_cache = dict(sorted(self.reports_cache.items()))
                json.dump(sorted_cache, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Failed to write cache: {e}")

    async def _search(
        self,
        query: str,
    ) -> List[dict]:
        """
        Perform a web search using all configured search engines.

        Args:
            query (str): The search query string.

        Returns:
            List[dict]: A list of unique search result objects (de-duplicated by URL).
        """
        search_tasks = [searcher.search(query) for searcher in self.searchers]
        results = await asyncio.gather(*search_tasks, return_exceptions=True)

        # Return search results and handle deduplication (2 identical URLs are given once)
        return list({item.url: item for sublist in results for item in sublist}.values())

    async def get_url_responses(self, urls: List[str]) -> List[requests.Response]:
        """
        Send HTTP GET requests to each URL to verify accessibility and content type.

        Args:
            urls (List[str]): A list of URL strings.

        Returns:
            List[bool | Exception]: A list where each entry is True (valid PDF), False, or an Exception.
        """
        async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:

            async def fetch(url):
                try:
                    async with session.get(
                        url, allow_redirects=True, timeout=aiohttp.ClientTimeout(total=30), ssl=False
                    ) as resp:
                        return (resp.status == 200) and (resp.content_type == "application/pdf")
                except Exception as e:
                    return e

            responses = await asyncio.gather(*(fetch(url) for url in urls), return_exceptions=True)
        return responses

    async def _format_urls(self, mne: dict, results: List[dict]) -> str:
        """
        Format the search results into a markdown-style string with numbered links,
        filtering for only accessible PDF URLs.

        Args:
            mne (dict): Dictionary representing a single MNE.
            results (List[dict]): List of search result objects.

        Returns:
            str: Formatted markdown string with accessible report candidates.
        """

        url_responses = await self.get_url_responses([str(r.url) for r in results])
        items = [
            f"{i}. [{r.title.strip()}]({r.url})\n{r.description.strip()}"
            for i, (r, resp) in enumerate(zip(results, url_responses))
            if resp
        ]
        block = "\n\n".join(items)
        return f"\n\n{block}"

    @observe()
    async def _call_llm(self, mne: dict, list_urls: str) -> Optional[AnnualReport]:
        """
        Call the LLM with the MNE name and list of candidate URLs to extract the correct one.

        Args:
            mne (dict): MNE information.
            list_urls (str): Formatted string of search results.

        Returns:
            Optional[AnnualReport]: Parsed annual report object or None if parsing fails.
        """
        # The prompt is stored in Langfuse so that it can be properly versionned
        messages = self.prompt.compile(mne_name=mne["NAME"], proposed_urls=list_urls)

        # Making the call to the LLM by specifying the message, the model to use, the format of the response and the temperature (very low to get consistent results)
        response = await self.client.beta.chat.completions.parse(
            name="annual_report_extractor",
            model=self.model,
            messages=messages,
            response_format=AnnualReport,
            temperature=0.1,
        )
        parsed = response.choices[0].message.parsed

        # Inject raw mne metadata
        parsed.mne_name = mne["NAME"]
        parsed.mne_id = mne["ID"]

        logger.debug(f"LLM parsed result for '{mne['NAME']}': {parsed}")
        return parsed

    async def async_fetch_for(self, mne: dict, web_query: str) -> Optional[AnnualReport]:
        """
        Asynchronously fetch the annual report for a single MNE.
        Uses search + LLM + validation pipeline.

        Args:
            mne (dict): MNE metadata.
            web_query (str): Search query string to find the annual report.

        Returns:
            Optional[AnnualReport]: Resulting annual report or None.
        """
        # Check if the MNE is already in the cache
        if mne["NAME"] in self.reports_cache:
            logger.info(f"Annual report for {mne['NAME']} already in cache.")
            # If in the cache, returns info from the cache
            return AnnualReport(
                mne_id=mne["ID"],
                mne_name=mne["NAME"],
                pdf_url=self.reports_cache[mne["NAME"]][1],
                year=self.reports_cache[mne["NAME"]][0],
            )
        try:
            # Make the websearch
            unrestricted_query = re.sub(r"\s*site:[^\s]+", "", web_query)
            # TODO: make a task in order to have 2 simultaneous searches
            results = await self._search(web_query)
            results += await self._search(unrestricted_query)
            if not results:
                return None
            # Format the urls obtained from the web search into a markdown prompt
            list_urls = await self._format_urls(mne, results)

            # Send the prompt to a LLM in order to find the best URL
            annual_report = await self._call_llm(mne, list_urls)

            # Update the cache with the new annual report
            if annual_report.pdf_url and annual_report.year >= 2024:
                self.reports_cache[annual_report.mne_name] = [annual_report.year, str(annual_report.pdf_url)]
                self._save_cache(self.CACHE_PATH)
            return annual_report
        except AssertionError as e:
            logger.error(f"Url extracted does not reply 200 response : {e}")
            return None

    def fetch_for(self, mne: dict, web_query: str) -> Optional[AnnualReport]:
        """
        Synchronous wrapper for `async_fetch_for`, for compatibility with sync code.

        Args:
            mne (dict): MNE metadata.
            web_query (str): Search query string to find the annual report.

        Returns:
            Optional[AnnualReport]: Resulting annual report or None.
        """
        return asyncio.run(self.async_fetch_for(mne, web_query))

    async def fetch_batch(self, mnes: List[dict], web_queries: List[str]) -> List[Optional[AnnualReport]]:
        """
        Asynchronously fetch annual reports for a list of MNEs.

        Args:
            mnes (List[dict]): List of MNEs.
            web_queries (List[str]): List of search queries corresponding to each MNE.

        Returns:
            List[Optional[AnnualReport]]: List of annual report results, one per MNE.
        """
        tasks = [self.async_fetch_for(mne, query) for mne, query in zip(mnes, web_queries)]
        return await tqdm.gather(*tasks)
