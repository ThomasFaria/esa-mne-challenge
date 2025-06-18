import asyncio
import os

import httpx
import nest_asyncio
import streamlit as st
from langfuse.openai import AsyncOpenAI

import config
from common.websearch.google import GoogleSearch
from extractors.utils import merge_extracted_infos
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
        searcher=[
            GoogleSearch(max_results=6),
        ],
        model="gemma3:27b",
        llm_client=llm_client,
    )
    # Extractors
    wiki_extractor = WikipediaExtractor(fetcher=wiki, client=client)
    yahoo_extractor = YahooExtractor(yahoo)

    classifier = NACEClassifier(
        llm_client=llm_client,
        model="gemma3:27b",
    )
    return client, wiki_extractor, yahoo_extractor, ar_fetcher, classifier, official_register


client, wiki_extractor, yahoo_extractor, ar_fetcher, classifier, official_register = init_services()


async def extract_initial_info(mne):
    yahoo_result, wiki_result = await asyncio.gather(
        yahoo_extractor.async_extract_for(mne),
        wiki_extractor.async_extract_for(mne),
    )
    return merge_extracted_infos(yahoo_result[0], wiki_result[0])


async def classify_activity(activity_desc, country, mne):
    mne_name = mne["NAME"]
    st.session_state[f"{mne_name}_activity_status"] = "Classifying..."

    country_spec = await official_register.async_fetch_for(mne, country=country)
    if country_spec and country_spec.mne_activity:
        nace_code = f"{classifier.mapping[country_spec.mne_activity]}{country_spec.mne_activity}"
    else:
        nace_code = await classifier.classify(activity_desc).code
    try:
        st.session_state[f"{mne_name}_classified_activity"] = nace_code
        st.session_state[f"{mne_name}_activity_status"] = "Done"
    except Exception as e:
        st.session_state[f"{mne_name}_activity_status"] = f"Error: {e}"


async def fetch_annual_report(mne, website_url):
    query = f"{mne['NAME']} annual report (2024 OR 2023) filetype:pdf {f'site:{website_url}' if website_url else ''}"
    try:
        result = await ar_fetcher.async_fetch_for(mne, web_query=query)
        if result and result.pdf_url:
            st.session_state[f"{mne['NAME']}_annual_pdf"] = result.pdf_url
    except Exception as e:
        st.session_state[f"{mne['NAME']}_annual_pdf"] = None
        st.session_state[f"{mne['NAME']}_pdf_status"] = f"Error: {e}"


def display_basic_info(info_merged, exclude_vars=None):
    info_dict = {item.variable: (item.value, item.year) for item in info_merged}
    for var in ["COUNTRY", "EMPLOYEES", "TURNOVER", "ASSETS", "WEBSITE"]:
        if exclude_vars and var in exclude_vars:
            continue
        val, year = info_dict.get(var, ("Not available", "N/A"))
        st.write(f"**{var.capitalize()}:** {val} (Year: {year})")
    return info_dict


async def orchestrate_workflow(mne_name):
    mne = {"NAME": mne_name, "ID": 0}
    info_merged = await extract_initial_info(mne)
    display_basic_info(info_merged, exclude_vars=["ACTIVITY"])

    activity_desc = next((item.value for item in info_merged if item.variable == "ACTIVITY"), None)
    website_url = next((item.value for item in info_merged if item.variable == "WEBSITE"), None)
    country = next((item.value for item in info_merged if item.variable == "COUNTRY"), None)
    tasks = []
    if activity_desc:
        tasks.append(classify_activity(activity_desc, country, mne))
    if website_url:
        tasks.append(fetch_annual_report(mne, website_url))

    if tasks:
        await asyncio.gather(*tasks)

    return info_merged


# Streamlit UI
st.title("MNE Info + Parallel Activity Classification + PDF Search")

mne_name_input = st.text_input("Enter the name of the Multinational Enterprise (MNE):")

if st.button("Extract Info") and mne_name_input:
    asyncio.run(orchestrate_workflow(mne_name_input))

    # Display ACTIVITY result if available
    act_state = st.session_state.get(f"{mne_name_input}_activity_status", "Waiting for classification...")
    classified_code = st.session_state.get(f"{mne_name_input}_classified_activity")

    if classified_code:
        st.success(f"**Activity Code (classified):** {classified_code}")
    else:
        st.info(f"**Activity:** {act_state}")

    # Display PDF preview link if available
    pdf_url = st.session_state.get(f"{mne_name_input}_annual_pdf")
    if pdf_url:
        st.header("Annual Report")
        st.markdown(f"[Download Annual Report]({pdf_url})")
        st.components.v1.iframe(pdf_url, height=600)
    else:
        st.info(st.session_state.get(f"{mne_name_input}_pdf_status", "Looking for annual report..."))
