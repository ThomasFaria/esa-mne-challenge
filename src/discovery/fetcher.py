import asyncio
import logging
from typing import List, Optional, Union

import aiohttp
import requests
from langfuse.decorators import observe
from langfuse.openai import AsyncOpenAI
from tqdm.asyncio import tqdm

from src.common.websearch.base import WebSearch
from src.discovery.models import AnnualReport
from src.discovery.prompts import SYS_PROMPT

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
        if isinstance(searcher, list):
            self.searchers = searcher
        else:
            self.searchers = [searcher]

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
    async def _call_llm(self, mne: dict, prompt: str) -> Optional[AnnualReport]:
        logger.info(f"Querying LLM for {mne['NAME']}")

        response = await self.client.beta.chat.completions.parse(
            model=self.model,
            messages=[
                {"role": "system", "content": SYS_PROMPT},
                {"role": "user", "content": prompt},
            ],
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
                        url, allow_redirects=True, timeout=aiohttp.ClientTimeout(total=5), ssl=False
                    ) as resp:
                        return resp.status
                except Exception as e:
                    return e

            responses = await asyncio.gather(*(fetch(url) for url in urls), return_exceptions=True)
        return responses

    async def _make_prompt(self, mne: dict, results: List[dict]) -> str:
        # Make a prompt for the LLM to extract the annual report URL
        # Make sure the URL is valid and the PDF is accessible

        url_responses = await self.get_url_responses([str(r.url) for r in results])
        items = [
            f"{i}. [{r.title.strip()}]({r.url})\n{r.description.strip()}"
            for i, (r, resp) in enumerate(zip(results, url_responses))
            if resp == 200
        ]
        block = "\n\n".join(items)
        return f"## Search Results for {mne['NAME']}\n\n{block}"

    async def async_fetch_for(self, mne: dict) -> Optional[AnnualReport]:
        """High-level: search + parse for one MNE name."""
        try:
            query = f"{mne['NAME']} annual report filetype:pdf"
            results = await self._search(query)
            if not results:
                return None
            prompt = await self._make_prompt(mne, results)
            annual_report = await self._call_llm(mne, prompt)
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
