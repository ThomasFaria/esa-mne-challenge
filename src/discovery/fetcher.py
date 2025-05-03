import logging
from typing import List, Optional

from duckduckgo_search import DDGS
from openai import OpenAI
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

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
    ):
        self.searcher = DDGS()
        self.client = OpenAI(api_key=api_key, base_url=base_url)
        self.model = model
        self.max_results = max_results

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(Exception),
        reraise=True,
    )
    def _search(self, mne: str) -> List[dict]:
        """Perform DuckDuckGo search with exponential-backoff retry."""
        logger.info(f"Searching for {mne} reports (max {self.max_results})")
        query = f"{mne} annual report filetype:pdf"
        results = list(self.searcher.text(query, max_results=self.max_results, region="us-en"))
        if not results:
            logger.warning(f"No results for {mne}")
        return results

    @retry(
        stop=stop_after_attempt(2),
        wait=wait_exponential(multiplier=1, min=1, max=5),
        retry=retry_if_exception_type(Exception),
        reraise=True,
    )
    def _call_llm(self, mne: str, prompt: str) -> Optional[AnnualReport]:
        """Call the LLM to parse out the PDF URL and year."""
        logger.info(f"Calling LLM for {mne}")
        response = self.client.beta.chat.completions.parse(
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
        items = []
        for r in results:
            title = r.get("title", "").strip()
            href = r.get("href", "").strip()
            body = r.get("body", "").strip().replace("\n", " ")
            items.append(f"[{title}]({href})\n{body}")
        block = "\n\n".join(items)
        return f"## Search Results for {mne}\n\n{block}"

    def fetch_for(self, mne: str) -> Optional[AnnualReport]:
        """High-level: search + parse for one MNE name."""
        results = self._search(mne)
        if not results:
            logger.error(f"Skipping {mne}: no search hits.")
            return None

        prompt = self._make_prompt(mne, results)
        try:
            return self._call_llm(mne, prompt)
        except Exception as e:
            logger.error(f"Failed LLM parsing for {mne}: {e}")
            return None
