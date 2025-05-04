import asyncio
import logging
from typing import List, Optional

from duckduckgo_search import DDGS
from openai import AsyncOpenAI
from tqdm.asyncio import tqdm

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
        self.searcher = DDGS()
        self.client = AsyncOpenAI(api_key=api_key, base_url=base_url)
        self.model = model
        self.max_results = max_results
        self._semaphore = asyncio.Semaphore(concurrency_limit)

    async def _search(self, mne: dict) -> List[dict]:
        """Perform DuckDuckGo search for annual report PDFs."""
        query = f"{mne['NAME']} annual report filetype:pdf"
        logger.info(f"Searching DuckDuckGo for '{query}'")

        results = await asyncio.to_thread(
            lambda: list(self.searcher.text(query, max_results=self.max_results, region="us-en"))
        )
        if not results:
            logger.warning(f"No search results for '{mne['NAME']}'")
        return results

    async def _call_llm(self, mne: dict, prompt: str) -> Optional[AnnualReport]:
        logger.info(f"Querying LLM for {mne['NAME']}")

        response = await self.client.beta.chat.completions.parse(
            model=self.model,
            messages=[
                {"role": "system", "content": SYS_PROMPT},
                {"role": "user", "content": prompt},
            ],
            response_format=AnnualReport,
        )
        parsed = response.choices[0].message.parsed

        # Inject raw mne metadata
        parsed.mne_name = mne["NAME"]
        parsed.mne_id = mne["ID"]

        logger.info(f"LLM parsed result for '{mne['NAME']}': {parsed}")
        return parsed

    def _make_prompt(self, mne: dict, results: List[dict]) -> str:
        items = [
            f"{i}. [{r.get('title', '').strip()}]({r.get('href', '').strip()})\n{r.get('body', '').strip().replace('\\n', ' ')}"
            for i, r in enumerate(results)
        ]
        block = "\n\n".join(items)
        return f"## Search Results for {mne['NAME']}\n\n{block}"

    async def async_fetch_for(self, mne: dict) -> Optional[AnnualReport]:
        """High-level: search + parse for one MNE name."""
        async with self._semaphore:
            try:
                results = await self._search(mne)
                if not results:
                    return None
                prompt = self._make_prompt(mne, results)
                return await self._call_llm(mne, prompt)
            except Exception as e:
                logger.error(f"Error fetching for '{mne['NAME']}': {e}")
                return None

    def fetch_for(self, mne: str) -> Optional[AnnualReport]:
        return asyncio.run(self.async_fetch_for(mne))
        # try:
        #     return asyncio.run(self.async_fetch_for(mne))
        # except RuntimeError as e:
        #     # Fallback for environments where event loop is already running
        #     logger.warning("Event loop already running; using asyncio.create_task workaround.")
        #     return asyncio.get_event_loop().run_until_complete(self.async_fetch_for(mne))

    async def fetch_batch(self, mnes: List[dict]) -> List[Optional[AnnualReport]]:
        """Fetch annual reports for a batch of MNEs asynchronously."""
        tasks = [self.async_fetch_for(mne) for mne in mnes]
        return await tqdm.gather(*tasks)
