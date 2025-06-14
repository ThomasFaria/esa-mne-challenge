import asyncio
import logging
import os

import httpx
from langfuse.openai import AsyncOpenAI
from tqdm import tqdm

import config
from common.data import generate_discovery_submission, generate_extraction_submission, load_mnes
from common.paths import DATA_DISCOVERY_PATH
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

# Configuration setup
config.setup()

# Logging setup
logger = logging.getLogger(__name__)

# Load MNE data from the challenge starting kit
mnes = load_mnes(DATA_DISCOVERY_PATH)

llm_client = AsyncOpenAI(
    base_url="https://llm.lab.sspcloud.fr/api",
    api_key=os.environ["OPENAI_API_KEY"],
)

yahoo = YahooFetcher()
official_register = OfficialRegisterFetcher()
ar_fetcher = AnnualReportFetcher(
    searcher=[
        GoogleSearch(max_results=6),
        # DuckDuckGoSearch(),
    ],
    model="gemma3:27b",
    llm_client=llm_client,
)

yahoo_extractor = YahooExtractor(yahoo)

classifier = NACEClassifier(
    llm_client=llm_client,
    model="gemma3:27b",
)


async def main():
    wiki = WikipediaFetcher()
    async with httpx.AsyncClient() as client:
        wiki_extractor = WikipediaExtractor(fetcher=wiki, client=client)
        pdf_extractor = PDFExtractor(client=client, llm_client=llm_client, model="gemma3:27b")

        extractions_results = []
        discovery_results = []

        for mne in tqdm(mnes, desc="Extracting MNEs"):
            try:
                # Extract data from Yahoo and Wikipedia
                logger.info(
                    f"Discovering Yahoo and Wikipedia pages for {mne['NAME']} and extracting available information..."
                )
                (yahoo_info, yahoo_sources), (wiki_info, wiki_source) = await asyncio.gather(
                    yahoo_extractor.async_extract_for(mne), wiki_extractor.async_extract_for(mne)
                )
                # Merge extracted information
                info_merged = merge_extracted_infos(yahoo_info, wiki_info)

                # Prepare the query for the annual report search
                website_url = next((item.value for item in info_merged if item.variable == "WEBSITE"), None)
                query = f"{mne['NAME']} annual report (2024 OR 2023) filetype:pdf {f'site:{website_url}' if website_url else ''}"

                # Fetch annual report
                logger.info(f"Fetching annual report for {mne['NAME']} with query: {query}")
                annual_report = await ar_fetcher.async_fetch_for(mne, web_query=query)

                country = next((item.value for item in info_merged if item.variable == "COUNTRY"), None)
                activity_desc = next((item.value for item in info_merged if item.variable == "ACTIVITY"), None)

                # Fetch country-specific details if needed
                logger.info(
                    f"Discovering and extracting country-specific URLs for {mne['NAME']} coming from {country}..."
                )
                country_spec = await official_register.async_fetch_for(mne, country=country)
                if country_spec and country_spec.mne_activity:
                    # Add section code before the division code
                    nace_code = f"{classifier.mapping[country_spec.mne_activity]}{country_spec.mne_activity}"
                else:
                    logger.info(f"Classifying activity for {mne['NAME']} using NACE classifier...")
                    activity = await classifier.classify(activity_desc)
                    nace_code = activity.code

                # Update activity code
                for item in info_merged:
                    if item.variable == "ACTIVITY":
                        item.value = nace_code

                # Handle missing variables (missing from Yahoo or Wikipedia or date older than 2023)
                VAR_TO_EXTRACT = ["COUNTRY", "EMPLOYEES", "TURNOVER", "ASSETS", "WEBSITE", "ACTIVITY"]
                var_missing = [
                    var
                    for var in VAR_TO_EXTRACT
                    if not any(item.variable == var and item.year >= 2023 for item in info_merged)
                ]
                if var_missing and annual_report.year >= 2024:
                    logger.info(f"Missing variables {var_missing}. Attempting to extract from annual report...")
                    # When some variables are missing, we try to extract from annual_report if available
                    pdf_infos = await pdf_extractor.async_extract_for(annual_report.pdf_url, var_missing)
                    info_merged.extend(pdf_extractor.extend_missing_vars(pdf_infos, mne, annual_report, var_missing))
                    info_merged = deduplicate_by_latest_year(info_merged)

                # Accumulate results
                sources = [
                    item
                    for item in ([annual_report] + (yahoo_sources or []) + [wiki_source] + [country_spec])
                    if item is not None
                ]

                discovery_results.append(sources)
                extractions_results.append(info_merged)

            except Exception as e:
                logger.exception(f"Error processing {mne['NAME']}: {e}")

        # Generate submissions once
        logger.info("Saving results for the discovery and the extraction challenge in the desired format...")
        generate_discovery_submission(discovery_results)
        generate_extraction_submission(extractions_results)
        return extractions_results, discovery_results


if __name__ == "__main__":
    # Configure logging
    logger.info("Starting the MNE extraction pipeline...")
    asyncio.run(main())
