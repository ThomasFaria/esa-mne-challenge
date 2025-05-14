import asyncio
import json
import logging
import os
from typing import List, Optional, Union

import aiohttp
import requests
from langfuse import Langfuse
from langfuse.decorators import observe
from langfuse.openai import AsyncOpenAI
from tqdm.asyncio import tqdm

from src.common.websearch.base import WebSearch
from src.discovery.models import AnnualReport

logger = logging.getLogger(__name__)


class AnnualReportFetcher:
    def __init__(
        self,
        searcher: Union[WebSearch, List[WebSearch]],
        api_key: str = "EMPTY",
        model: str = "mistralai/Mistral-Small-24B-Instruct-2501",
        base_url: str = "https://vllm-generation.user.lab.sspcloud.fr/v1",
    ):
        self.client = AsyncOpenAI(api_key=api_key, base_url=base_url)
        self.model = model
        self.prompt = Langfuse().get_prompt("annual-report-extractor", label="production")
        self.CACHE_PATH = "cache/reports_cache.json"
        self.reports_cache = self._load_cache(self.CACHE_PATH)

        if isinstance(searcher, list):
            self.searchers = searcher
        else:
            self.searchers = [searcher]

    def _load_cache(self, cache_path: str) -> dict:
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
        """Perform Web search for annual report PDFs."""

        search_tasks = [searcher.search(query) for searcher in self.searchers]
        results = await asyncio.gather(*search_tasks, return_exceptions=True)

        # Return flattened results
        return list({item.url: item for sublist in results for item in sublist}.values())

    @observe()
    async def _call_llm(self, mne: dict, list_urls: str) -> Optional[AnnualReport]:
        logger.info(f"Querying LLM for {mne['NAME']}")

        messages = self.prompt.compile(mne_name=mne["NAME"], proposed_urls=list_urls)

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

        logger.info(f"LLM parsed result for '{mne['NAME']}': {parsed}")
        return parsed

    async def get_url_responses(self, urls: List[str]) -> List[requests.Response]:
        """Get responses for a list of URLs."""
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
        # Make a prompt for the LLM to extract the annual report URL
        # Make sure the URL is valid and the PDF is accessible

        url_responses = await self.get_url_responses([str(r.url) for r in results])
        items = [
            f"{i}. [{r.title.strip()}]({r.url})\n{r.description.strip()}"
            for i, (r, resp) in enumerate(zip(results, url_responses))
            if resp
        ]
        block = "\n\n".join(items)
        return f"\n\n{block}"

    async def async_fetch_for(self, mne: dict) -> Optional[AnnualReport]:
        """High-level: search + parse for one MNE name."""
        # Check if the MNE is already in the cache
        if mne["NAME"] in self.reports_cache:
            logger.info(f"Annual report for {mne['NAME']} already in cache.")
            return AnnualReport(
                mne_id=mne["ID"],
                mne_name=mne["NAME"],
                pdf_url=self.reports_cache[mne["NAME"]][1],
                year=self.reports_cache[mne["NAME"]][0],
            )
        try:
            query = f"{mne['NAME']} annual report (2024 OR 2023) filetype:pdf"
            results = await self._search(query)
            if not results:
                return None
            list_urls = await self._format_urls(mne, results)
            annual_report = await self._call_llm(mne, list_urls)

            # Update the cache with the new annual report
            if annual_report.pdf_url and annual_report.year >= 2024:
                self.reports_cache[annual_report.mne_name] = [annual_report.year, str(annual_report.pdf_url)]
                self._save_cache(self.CACHE_PATH)
            return annual_report
        except AssertionError as e:
            logger.error(f"Url extracted does not reply 200 response : {e}")
            return None

    def fetch_for(self, mne: dict) -> Optional[AnnualReport]:
        return asyncio.run(self.async_fetch_for(mne))

    async def fetch_batch(self, mnes: List[dict]) -> List[Optional[AnnualReport]]:
        """Fetch annual reports for a batch of MNEs asynchronously."""
        tasks = [self.async_fetch_for(mne) for mne in mnes]
        return await tqdm.gather(*tasks)
