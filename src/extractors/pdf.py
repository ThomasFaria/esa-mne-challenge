import asyncio
import logging
import re
from collections import defaultdict
from typing import Optional

import fitz  # PyMuPDF
import httpx
import iso4217parse
import pycountry
from langfuse import Langfuse
from langfuse.decorators import observe
from langfuse.openai import AsyncOpenAI

from extractors.models import ExtractedInfo, PDFExtractionResult
from fetchers.models import AnnualReport

logger = logging.getLogger(__name__)

KEYWORD_MAP = {
    "TURNOVER": [r"\b(revenue|total\s+sales|turnover)\b"],
    "ASSETS": [r"\b(total\s+assets|assets)\b"],
    "EMPLOYEES": [r"\bemployees\b"],
}

MIN_REQUIRED_PAGES = 2  # Min pages per variable
MAX_TOTAL_PAGES = 10  # Max total pages selected


class PDFExtractor:
    """
    Extractor for parsing and extracting structured data from PDFs.
    """

    def __init__(
        self,
        client: httpx.AsyncClient,
        llm_client: AsyncOpenAI,
        model: str = "mistralai/Mistral-Small-24B-Instruct-2501",
    ):
        """
        Initialize the PDFExtractor with a language model client.
        Args:
            llm_client: Client that can process unstructured text and return structured data.
        """
        self.client = client
        self.llm_client = llm_client
        self.model = model
        self.prompt = Langfuse().get_prompt("pdf-extractor", label="production")

    async def extract_pdf_infos(self, pdf_url: str) -> Optional[PDFExtractionResult]:
        """
        Extract structured data from a PDF.

        Args:
            pdf_url (str): URL to the PDF file.

        Returns:
            Optional[PDFExtractionResult]: Structured extracted variables or None.
        """

        try:
            # 1. Download PDF and extract text
            doc = await self.download_pdf(pdf_url)
            if not doc:
                logger.warning("No text extracted from PDF")
                return None

            # 2. Extract text and identify relevant pages
            selected_pages = self.select_top_pages(doc)

            # 3. Process with LLM for structured extraction
            response = await self._call_llm(selected_pages)

            # 3. Wrap results
            response.country = (
                pycountry.countries.search_fuzzy(response.country)[0].alpha_2 if response.country else None
            )
            response.currency = iso4217parse.parse(response.currency)[0].alpha3 if response.currency else None
            return response

        except Exception as e:
            logger.warning(f"No text extracted from PDF: {e}")
            return None
        finally:
            doc.close()

    async def download_pdf(self, url: str) -> str:
        """
        Downloads a PDF from a URL and extracts its text content.

        Args:
            url (str): The URL of the PDF.

        Returns:
            str: The extracted text.
        """
        try:
            response = await self.client.get(url)
            return fitz.open(stream=response.content, filetype="pdf")

        except Exception as e:
            logger.error(f"Failed to extract PDF from {url}: {e}")
            return ""

    def extract_variable_page_map(self, doc):
        page_data = []  # List of dicts: one per page

        for i, page in enumerate(doc):
            blocks = page.get_text("blocks")
            text_blocks = []

            for block in blocks:
                if len(block) > 4 and isinstance(block[4], str):
                    cleaned = " ".join(line.strip() for line in block[4].split("\n"))
                    text_blocks.append(cleaned)

            page_text = "\n".join(text_blocks)
            page_text_lower = page_text.lower()

            matched_variables = set()
            for variable, patterns in KEYWORD_MAP.items():
                for pattern in patterns:
                    if re.search(pattern, page_text_lower):
                        matched_variables.add(variable)
                        break

            if matched_variables:
                page_data.append(
                    {
                        "page_number": i + 1,
                        "text": page_text,
                        "matched_variables": matched_variables,
                        "match_count": len(matched_variables),
                    }
                )

        return page_data

    def select_top_pages(self, doc):
        page_data = self.extract_variable_page_map(doc)

        # Step 1: rank pages by number of matched variables (descending), then page number
        ranked_pages = sorted(page_data, key=lambda p: (-p["match_count"], p["page_number"]))

        selected = []
        selected_page_numbers = set()
        variable_coverage = defaultdict(int)

        # Step 2: pick top-ranked pages
        for page in ranked_pages:
            if len(selected) >= MAX_TOTAL_PAGES:
                break

            selected.append(page)
            selected_page_numbers.add(page["page_number"])

            for var in page["matched_variables"]:
                variable_coverage[var] += 1

        # Step 3: ensure each variable is represented at least MIN_REQUIRED_PAGES times
        for var in KEYWORD_MAP.keys():
            if variable_coverage[var] >= MIN_REQUIRED_PAGES:
                continue

            # Add more pages specifically for this variable
            for page in ranked_pages:
                if page["page_number"] in selected_page_numbers:
                    continue
                if var in page["matched_variables"]:
                    selected.append(page)
                    selected_page_numbers.add(page["page_number"])
                    variable_coverage[var] += 1
                    if variable_coverage[var] >= MIN_REQUIRED_PAGES:
                        break

        return selected

    @observe()
    async def _call_llm(self, text: str) -> Optional[PDFExtractionResult]:
        # The prompt is stored in Langfuse so that it can be properly versionned
        messages = self.prompt.compile(text=text)

        # Making the call to the LLM by specifying the message, the model to use, the format of the response and the temperature (very low to get consistent results)
        response = await self.llm_client.beta.chat.completions.parse(
            name="pdf_infos_extractor",
            model=self.model,
            messages=messages,
            response_format=PDFExtractionResult,
            temperature=0.1,
        )
        parsed = response.choices[0].message.parsed
        return parsed

    def extend_missing_vars(
        pdf_infos: PDFExtractionResult, mne: dict, annual_report: AnnualReport, var_missing: list
    ) -> list[ExtractedInfo]:
        """
        Create ExtractedInfo objects for missing variables based on PDF content.

        Args:
            pdf_infos (PDFExtractionResult): Extracted structured data from PDF.
            mne (dict): MNE information containing ID and NAME.
            annual_report (AnnualReport): Annual report metadata.
            var_missing (list): List of variable names that are missing.

        Returns:
            List[ExtractedInfo]
        """
        # Map variable names to attributes in pdf_infos
        attr_map = {
            "EMPLOYEES": "employees",
            "TURNOVER": "turnover",
            "ASSETS": "assets",
        }

        extracted = []

        for var in var_missing:
            attr_name = attr_map.get(var)
            value = getattr(pdf_infos, attr_name, None)

            if value is not None:
                extracted.append(
                    ExtractedInfo(
                        mne_id=mne["ID"],
                        mne_name=mne["NAME"],
                        variable=var,
                        source_url=annual_report.pdf_url,
                        value=value,
                        currency=pdf_infos.currency,
                        year=annual_report.year,
                    )
                )

        return extracted

    async def async_extract_for(self, pdf_url: str) -> Optional[PDFExtractionResult]:
        """
        Async wrapper to extract structured data from PDF for a given MNE.

        Args:
            pdf_url (str): URL to the PDF file.

        Returns:
            Optional[PDFExtractionResult]: Extracted structured data or None.
        """
        infos = await self.extract_pdf_infos(pdf_url)
        return infos

    def extract_for(self, pdf_url: str) -> Optional[PDFExtractionResult]:
        """
        Sync wrapper for extracting data from PDF.

        Args:
            pdf_url (str): URL to the PDF file.

        Returns:
            Optional[PDFExtractionResult]: Extracted structured data or None.
        """
        return asyncio.run(self.async_extract_for(pdf_url))
