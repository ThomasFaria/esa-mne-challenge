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


mne_name = "ETEX"
mne = {"ID": 0000, "NAME": mne_name}
mne = mnes[91]
extractions_results = []
discovery_results = []


for mne in tqdm(mnes, desc="Extracting MNEs"):
    yahoo_info, yahoo_sources = yahoo_extractor.extract_for(mne)
    wiki_info, wiki_sources = wiki_extractor.extract_for(mne)
    info_merged = merge_extracted_infos(yahoo_info, wiki_info)

    website_url = next((item.value for item in info_merged if item.variable == "WEBSITE"), None)
    ### Search AR based on website
    site_filter = (
        f"site:{website_url.replace('https://', '').replace('http://', '').replace('www.', '')}" if website_url else ""
    )
    query = f"{mne['NAME']} annual report (2024 OR 2023) filetype:pdf {site_filter}"
    annual_report = ar_fetcher.fetch_for(mne, web_query=query)

    country = next((item.value for item in info_merged if item.variable == "COUNTRY"), None)
    activity_desc = next((item.value for item in info_merged if item.variable == "ACTIVITY"), None)

    if country == "FR":
        country_spec = official_register.fetch_for(mne, country=country)
        if country_spec.mne_activity is not None:
            nace_code = country_spec.mne_activity
            next(
                item for item in info_merged if item.variable == "ACTIVITY"
            ).value = f"{classifier.mapping[nace_code]}{nace_code}"
        else:
            activity = classifier.classify(activity_desc)
            next(item for item in info_merged if item.variable == "ACTIVITY").value = activity.code
    else:
        activity = classifier.classify(activity_desc)
        next(item for item in info_merged if item.variable == "ACTIVITY").value = activity.code
        country_spec = None

    if len(info_merged) != 6:
        VAR_TO_EXTRACT = [
            "COUNTRY",
            "EMPLOYEES",
            "TURNOVER",
            "ASSETS",
            "WEBSITE",
            "ACTIVITY",
        ]
        var_missing = [var for var in VAR_TO_EXTRACT if var not in [item.variable for item in info_merged]]
        logger.warning(f"Missing variables for {mne_name}: {var_missing}")
        # Load PDF of annual report and extract missing variables using LLM

    discovery_results.append([annual_report, *yahoo_sources, wiki_sources, country_spec])
    extractions_results.append(info_merged)

generate_discovery_submission(discovery_results)
generate_extraction_submission(extractions_results)

# faire streamlit qui cherche pour 1 entreprise à la fois
# checker wikipedia pour corriger les infos utiliser les vrais infobox
