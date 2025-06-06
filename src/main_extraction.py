import logging
import os

from tqdm import tqdm

import config
from common.data import generate_discovery_submission, generate_extraction_submission, load_mnes
from common.paths import DATA_DISCOVERY_PATH
from common.websearch.google import GoogleSearch
from extraction.utils import merge_extracted_infos
from extraction.wikipedia import WikipediaExtractor
from extraction.yahoo import YahooExtractor
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

wiki = WikipediaFetcher()
yahoo = YahooFetcher()
official_register = OfficialRegisterFetcher()
ar_fetcher = AnnualReportFetcher(
    searcher=[
        GoogleSearch(max_results=10),
        # DuckDuckGoSearch(),
    ],
    model="gemma3:27b",
    base_url="https://llm.lab.sspcloud.fr/api",
    api_key=os.environ["OPENAI_API_KEY"],
)

wiki_extractor = WikipediaExtractor(wiki)
yahoo_extractor = YahooExtractor(yahoo)

classifier = NACEClassifier(
    base_url="https://llm.lab.sspcloud.fr/api",
    api_key=os.environ["OPENAI_API_KEY"],
)


def format_site_filter(url: str) -> str:
    if url:
        domain = url.replace("https://", "").replace("http://", "").replace("www.", "")
        return f"site:{domain}"
    return ""


async def main():
    extractions_results = []
    discovery_results = []

    for mne in tqdm(mnes, desc="Extracting MNEs"):
        try:
            # Extract data from Yahoo and Wikipedia
            logger.info(
                f"Discovering Yahoo and Wikipedia pages for {mne['NAME']} and extracting available information..."
            )
            yahoo_info, yahoo_sources = await yahoo_extractor.async_extract_for(mne)
            wiki_info, wiki_sources = await wiki_extractor.async_extract_for(mne)

            info_merged = merge_extracted_infos(yahoo_info, wiki_info)

            # Prepare the query for the annual report search
            website_url = next((item.value for item in info_merged if item.variable == "WEBSITE"), None)
            site_filter = format_site_filter(website_url)
            query = f"{mne['NAME']} annual report (2024 OR 2023) filetype:pdf {site_filter}"

            # Fetch annual report
            logger.info(f"Fetching annual report for {mne['NAME']} with query: {query}")
            annual_report = await ar_fetcher.async_fetch_for(mne, web_query=query)

            country = next((item.value for item in info_merged if item.variable == "COUNTRY"), None)
            activity_desc = next((item.value for item in info_merged if item.variable == "ACTIVITY"), None)

            # Fetch country-specific details if needed
            logger.info(f"Discovering and extracting country-specific URLs for {mne['NAME']} coming from {country}...")
            country_spec = await official_register.async_fetch_for(mne, country=country)
            if country_spec and country_spec.mne_activity:
                nace_code = country_spec.mne_activity
            else:
                logger.info(f"Classifying activity for {mne['NAME']} using NACE classifier...")
                activity = classifier.classify(activity_desc)
                nace_code = activity.code

            # Update activity code
            for item in info_merged:
                if item.variable == "ACTIVITY":
                    item.value = nace_code

            # Handle missing variables
            VAR_TO_EXTRACT = ["COUNTRY", "EMPLOYEES", "TURNOVER", "ASSETS", "WEBSITE", "ACTIVITY"]
            var_missing = [var for var in VAR_TO_EXTRACT if var not in [item.variable for item in info_merged]]

            if var_missing:
                logger.info(f"Missing variables {var_missing}. Attempting to extract from annual report...")
                # Additional logic to extract from annual_report if available

            # Accumulate results
            logger.info("Saving results for the discovery and the extraction challenge in the desired format...")
            discovery_results.append([annual_report, *yahoo_sources, wiki_sources, country_spec])
            extractions_results.append(info_merged)

        except Exception as e:
            logger.exception(f"Error processing {mne['NAME']}: {e}")

    # Generate submissions once
    generate_discovery_submission(discovery_results)
    generate_extraction_submission(extractions_results)
    return extractions_results, discovery_results
