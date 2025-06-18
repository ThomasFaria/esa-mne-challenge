import asyncio
import os

import httpx
import nest_asyncio
import streamlit as st
from langfuse.openai import AsyncOpenAI

import config
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

nest_asyncio.apply()
config.setup()


@st.cache_resource
def init_services():
    client = httpx.AsyncClient()
    llm_client = AsyncOpenAI(
        base_url="https://llm.lab.sspcloud.fr/api",
        api_key=os.environ["OPENAI_API_KEY"],
    )
    # Fetchers
    wiki = WikipediaFetcher()
    yahoo = YahooFetcher()
    official_register = OfficialRegisterFetcher()
    ar_fetcher = AnnualReportFetcher(
        searcher=[GoogleSearch(max_results=6)],
        model="gemma3:27b",
        llm_client=llm_client,
    )
    # Extractors
    wiki_extractor = WikipediaExtractor(fetcher=wiki, client=client)
    yahoo_extractor = YahooExtractor(yahoo)
    pdf_extractor = PDFExtractor(client=client, llm_client=llm_client, model="gemma3:27b")

    classifier = NACEClassifier(
        llm_client=llm_client,
        model="gemma3:27b",
    )
    return {
        "client": client,
        "wiki_extractor": wiki_extractor,
        "yahoo_extractor": yahoo_extractor,
        "pdf_extractor": pdf_extractor,
        "ar_fetcher": ar_fetcher,
        "classifier": classifier,
        "official_register": official_register,
    }


# initialize services
services = init_services()
client = services["client"]
wiki_extractor = services["wiki_extractor"]
yahoo_extractor = services["yahoo_extractor"]
pdf_extractor = services["pdf_extractor"]
ar_fetcher = services["ar_fetcher"]
classifier = services["classifier"]
official_register = services["official_register"]


async def extract_initial_info(mne):
    st.info("Extracting initial information from Yahoo and Wikipedia...")
    yahoo_res, wiki_res = await asyncio.gather(
        yahoo_extractor.async_extract_for(mne),
        wiki_extractor.async_extract_for(mne),
    )
    return merge_extracted_infos(yahoo_res[0], wiki_res[0])


async def classify_activity(activity_desc, country, mne_name, mne):
    st.session_state[f"{mne_name}_activity_status"] = "Classifying..."
    st.info("Classifying the activity using official registers or LLM classifier...")
    country_spec = await official_register.async_fetch_for(mne, country=country)
    if country_spec and country_spec.mne_activity:
        code = f"{classifier.mapping[country_spec.mne_activity]}{country_spec.mne_activity}"
    else:
        code = (await classifier.classify(activity_desc)).code
    st.session_state[f"{mne_name}_classified_activity"] = code
    st.session_state[f"{mne_name}_activity_status"] = "Done"


async def fetch_annual_report(mne, mne_name, website_url):
    st.session_state[f"{mne_name}_pdf_status"] = "Searching report..."
    st.info("Fetching annual report from the web...")
    query = f"{mne_name} annual report (2024 OR 2023) filetype:pdf {f'site:{website_url}' if website_url else ''}"
    report = await ar_fetcher.async_fetch_for(mne, web_query=query)
    if report and report.pdf_url:
        st.session_state[f"{mne_name}_annual_pdf"] = report
        st.session_state[f"{mne_name}_pdf_status"] = "Found"
    else:
        st.session_state[f"{mne_name}_annual_pdf"] = None
        st.session_state[f"{mne_name}_pdf_status"] = "Not found"


def display_basic_info(info_merged, exclude_vars=None):
    info_dict = {item.variable: (item.value, item.year) for item in info_merged}
    for var in ["COUNTRY", "EMPLOYEES", "TURNOVER", "ASSETS", "WEBSITE"]:
        if exclude_vars and var in exclude_vars:
            continue
        val, year = info_dict.get(var, ("Not available", None))
        status = f"(Year: {year})" if year else ""
        st.write(f"**{var.capitalize()}:** {val} {status}")
    return info_dict


async def orchestrate_workflow(mne_name):
    mne = {"NAME": mne_name, "ID": 0}
    # 1) extract initial info
    info_merged = await extract_initial_info(mne)
    display_basic_info(info_merged, exclude_vars=["ACTIVITY"])

    # 2) schedule classification and report fetch
    activity_desc = next((i.value for i in info_merged if i.variable == "ACTIVITY"), None)
    website_url = next((i.value for i in info_merged if i.variable == "WEBSITE"), None)
    country = next((i.value for i in info_merged if i.variable == "COUNTRY"), None)
    tasks = []
    if activity_desc:
        tasks.append(classify_activity(activity_desc, country, mne_name, mne))
    if website_url:
        tasks.append(fetch_annual_report(mne, mne_name, website_url))
    if tasks:
        st.info("Running classification and fetching the annual report...")
        await asyncio.gather(*tasks)

    # 3) handle missing or outdated vars via PDF
    # Determine vars missing or older than 2023
    missing = [
        v
        for v in ["COUNTRY", "EMPLOYEES", "TURNOVER", "ASSETS", "WEBSITE", "ACTIVITY"]
        if not any(i.variable == v and (i.year or 0) >= 2023 for i in info_merged)
    ]
    # if missing and we have a newer report
    report = st.session_state.get(f"{mne_name}_annual_pdf")
    if missing and report and report.year >= 2024:
        st.info(f"Extracting {missing} from PDF... this may take a moment.")
        pdf_infos = await pdf_extractor.async_extract_for(report.pdf_url, missing)
        info_merged.extend(pdf_extractor.extend_missing_vars(pdf_infos, mne, report, missing))
        info_merged = deduplicate_by_latest_year(info_merged)
        st.success("PDF extraction complete.")
    elif missing:
        st.warning("Some variables are missing or outdated, but no recent report available.")

    # 4) final display including activity
    display_basic_info(info_merged)
    # show classified activity code if present
    code = st.session_state.get(f"{mne_name}_classified_activity")
    if code:
        st.success(f"**Activity Code:** {code}")
    # show report viewer
    report = st.session_state.get(f"{mne_name}_annual_pdf")
    if report and report.pdf_url:
        st.header("Annual Report Preview")
        st.markdown(f"[Download Report]({report.pdf_url})")
    else:
        st.info(st.session_state.get(f"{mne_name}_pdf_status"))


# --- Streamlit UI ---
st.title("MNE Info Explorer")
mne_input = st.text_input("MNE Name:")
if st.button("Run Extraction") and mne_input:
    asyncio.run(orchestrate_workflow(mne_input))
