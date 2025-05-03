import asyncio
import logging
from typing import List, Optional

from duckduckgo_search import AsyncDDGS
from openai import AsyncOpenAI

from src.discovery.models import AnnualReport
from src.discovery.prompts import SYS_PROMPT

logger = logging.getLogger(__name__)


class AnnualReportFetcher:
    def __init__(
        self,
        api_key: str = "EMPTY",
        model: str = "mistralai/Mistral-Small-24B-Instruct-2501",
        base_url: str = "https://vllm-generation.user.lab.sspcloud.fr/v1",
        max_results: int = 6,
        concurrency_limit: int = 5,
    ):
        self.searcher = AsyncDDGS()
        self.client = AsyncOpenAI(api_key=api_key, base_url=base_url)
        self.model = model
        self.max_results = max_results
        self._semaphore = asyncio.Semaphore(concurrency_limit)

    async def _search(self, mne: str) -> List[dict]:
        """Perform DuckDuckGo search for annual report PDFs."""
        logger.info(f"Searching DuckDuckGo for {mne} reports")
        query = f"{mne} annual report filetype:pdf"
        results = await self.searcher.text(query, max_results=self.max_results, region="us-en")
        results = list(results or [])
        if not results:
            logger.warning(f"No results for {mne}")
        return results

    async def _call_llm(self, mne: str, prompt: str) -> Optional[AnnualReport]:
        """Call the LLM to parse out the PDF URL and year."""
        logger.info(f"Querying LLM for {mne}")
        response = await self.client.beta.chat.completions.parse(
            model=self.model,
            messages=[
                {"role": "system", "content": SYS_PROMPT},
                {"role": "user", "content": prompt},
            ],
            response_format=AnnualReport,
        )
        parsed = response.choices[0].message.parsed
        logger.info(f"LLM returned: {parsed.json()}")
        return parsed

    def _make_prompt(self, mne: str, results: List[dict]) -> str:
        """Turn raw search results into the markdown prompt expected by the LLM."""
        items = [
            f"{i}. [{r.get('title', '').strip()}]({r.get('href', '').strip()})\n{r.get('body', '').strip().replace('\\n', ' ')}"
            for i, r in enumerate(results)
        ]
        block = "\n\n".join(items)
        return f"## Search Results for {mne}\n\n{block}"

    async def fetch_for(self, mne: str) -> Optional[AnnualReport]:
        """High-level: search + parse for one MNE name."""
        async with self._semaphore:
            try:
                results = await self._search(mne)
                if not results:
                    return None
                prompt = self._make_prompt(mne, results)
                return await self._call_llm(mne, prompt)
            except Exception as e:
                logger.error(f"Error fetching for '{mne}': {e}")
                return None

    async def fetch_batch(self, mnes: List[str]) -> List[Optional[AnnualReport]]:
        """Fetch annual reports for a batch of MNEs asynchronously."""
        tasks = [self.fetch_for(mne) for mne in mnes]
        return await asyncio.gather(*tasks)
