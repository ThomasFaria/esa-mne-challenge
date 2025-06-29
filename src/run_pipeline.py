"""
Main script for running the ESA-MNE Challenge data pipeline.

Steps:
1. Load MNE targets.
2. Fetch company data from Yahoo & Wikipedia.
3. Search and fetch annual reports (PDFs).
4. When not found in Yahoo or wikipedia extract structured data from annual reports using LLM.
5. Classify company activity using country specific sources or RAG.
7. Output formatted csv files for challenge submission.
"""

import asyncio
import logging
import os

import httpx
from langfuse.openai import AsyncOpenAI
from tqdm import tqdm

import config
from common.data import generate_discovery_submission, generate_extraction_submission, load_mnes
from common.paths import DATA_DISCOVERY_PATH
from common.websearch.duckduckgo import DuckDuckGoSearch
from common.websearch.google import GoogleSearch
from extractors.pdf import PDFExtractor
from extractors.utils import deduplicate_by_latest_year, merge_extracted_infos
from extractors.wikipedia import WikipediaExtractor
from extractors.yahoo import YahooExtractor
from fetchers.annual_reports import AnnualReportFetcher
from fetchers.official_register import OfficialRegisterFetcher
from fetchers.wikipedia import WikipediaFetcher
from fetchers.yahoo import YahooFetcher
from nace_classifier.classifier import NACEClassifier

# ---------------------------
# Configuration and Logging
# ---------------------------
config.setup()
logger = logging.getLogger(__name__)

# ---------------------------
# Load MNE Discovery Dataset
# ---------------------------
mnes = load_mnes(DATA_DISCOVERY_PATH)

# ---------------------------
# Initialize Clients and Models
# ---------------------------
llm_client = AsyncOpenAI(
    base_url="https://llm.lab.sspcloud.fr/api",
    api_key=os.environ["OPENAI_API_KEY"],
)

# Data Fetchers
yahoo = YahooFetcher()
official_register = OfficialRegisterFetcher()
wiki = WikipediaFetcher()

# Annual report fetcher using web search + LLM
ar_fetcher = AnnualReportFetcher(
    searcher=[GoogleSearch(max_results=6), DuckDuckGoSearch(max_results=6)],
    model="gemma3:27b",
    llm_client=llm_client,
)

# Data extractors
yahoo_extractor = YahooExtractor(yahoo)
classifier = NACEClassifier(llm_client=llm_client, model="gemma3:27b")


async def main():
    """
    Run the complete MNE extraction pipeline asynchronously.
    """
    async with httpx.AsyncClient() as client:
        # PDF and Wikipedia data extractors
        wiki_extractor = WikipediaExtractor(fetcher=wiki, client=client)
        pdf_extractor = PDFExtractor(client=client, llm_client=llm_client, model="gemma3:27b")

        extractions_results = []
        discovery_results = []

        # Iterate through MNEs
        for mne in tqdm(mnes, desc="Extracting MNEs"):
            try:
                # STEP 1: Extract Yahoo and Wikipedia data
                logger.info(f"Extracting public info for {mne['NAME']}...")
                (yahoo_info, yahoo_sources), (wiki_info, wiki_source) = await asyncio.gather(
                    yahoo_extractor.async_extract_for(mne),
                    wiki_extractor.async_extract_for(mne),
                )

                # STEP 2: Merge retrieved info (keep the most recent, if both from 2024 => keep Yahoo)
                info_merged = merge_extracted_infos(yahoo_info, wiki_info)

                # STEP 3: Build query for annual reports (adds `site:` if website is known)
                website_url = next((item.value for item in info_merged if item.variable == "WEBSITE"), None)
                query = f"{mne['NAME']} annual report (2024 OR 2023) filetype:pdf {f'site:{website_url}' if website_url else ''}"

                logger.info(f"Searching for annual report: {query}")
                annual_report = await ar_fetcher.async_fetch_for(mne, web_query=query)

                # STEP 4: Classify NACE code (either via official register or RAG)
                country = next((item.value for item in info_merged if item.variable == "COUNTRY"), None)
                activity_desc = next((item.value for item in info_merged if item.variable == "ACTIVITY"), None)

                logger.info(f"Fetching official register info for {mne['NAME']}...")
                country_spec = await official_register.async_fetch_for(mne, country=country)

                if country_spec and country_spec.mne_activity:
                    # Add section code before the division code
                    nace_code = f"{classifier.mapping[country_spec.mne_activity]}{country_spec.mne_activity}"
                else:
                    logger.info(f"Classifying activity for {mne['NAME']} using RAG...")
                    activity = await classifier.classify(activity_desc)
                    nace_code = activity.code

                # STEP 5: Update the activity variable with NACE code
                for item in info_merged:
                    if item.variable == "ACTIVITY":
                        item.value = nace_code

                # STEP 6: Fill missing values via annual report if available
                VAR_TO_EXTRACT = [
                    "COUNTRY",
                    "EMPLOYEES",
                    "TURNOVER",
                    "ASSETS",
                    "WEBSITE",
                ]
                var_missing = [
                    var
                    for var in VAR_TO_EXTRACT
                    if not any(item.variable == var and item.year >= 2023 for item in info_merged)
                ]

                if var_missing and annual_report.year >= 2024:
                    logger.info(f"Attempting to extract missing vars {var_missing} from PDF...")
                    pdf_infos = await pdf_extractor.async_extract_for(annual_report.pdf_url, var_missing)
                    info_merged.extend(pdf_extractor.extend_missing_vars(pdf_infos, mne, annual_report, var_missing))
                    info_merged = deduplicate_by_latest_year(info_merged)

                # STEP 7: Track discovery sources
                sources = [
                    item
                    for item in ([annual_report] + (yahoo_sources or []) + [wiki_source] + [country_spec])
                    if item is not None
                ]
                discovery_results.append(sources)
                extractions_results.append(info_merged)

            except Exception as e:
                logger.exception(f"Error processing {mne['NAME']}: {e}")

        # STEP 8: Save results for submission for both challenges (Discovery & Extraction)
        logger.info("Generating final submission files...")
        generate_discovery_submission(discovery_results)
        generate_extraction_submission(extractions_results)

        return extractions_results, discovery_results


if __name__ == "__main__":
    logger.info("Starting the MNE extraction pipeline...")
    asyncio.run(main())
