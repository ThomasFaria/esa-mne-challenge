import asyncio
import os

import streamlit as st

import config
from common.websearch.google import GoogleSearch
from extractors.utils import merge_extracted_infos
from extractors.wikipedia import WikipediaExtractor
from extractors.yahoo import YahooExtractor
from fetchers.annual_reports import AnnualReportFetcher
from fetchers.wikipedia import WikipediaFetcher
from fetchers.yahoo import YahooFetcher

config.setup()

# Initialize fetchers
wiki_fetcher = WikipediaFetcher()
yahoo_fetcher = YahooFetcher()
wiki_extractor = WikipediaExtractor(wiki_fetcher)
yahoo_extractor = YahooExtractor(yahoo_fetcher)
ar_fetcher = AnnualReportFetcher(
    searcher=[GoogleSearch(max_results=10)],
    model="gemma3:27b",
    base_url="https://llm.lab.sspcloud.fr/api",
    api_key=os.environ["OPENAI_API_KEY"],
)


# Async helper functions
async def fetch_financial_info(mne_name: str):
    mne = {"NAME": mne_name, "ID": 0}
    yahoo_info, _ = await yahoo_extractor.async_extract_for(mne)
    wiki_info, _ = await wiki_extractor.async_extract_for(mne)
    info_merged = merge_extracted_infos(yahoo_info, wiki_info)
    return info_merged


async def fetch_annual_report_pdf(mne_name: str, website_url: str):
    site_filter = (
        f"site:{website_url.replace('https://', '').replace('http://', '').replace('www.', '')}" if website_url else ""
    )
    query = f"{mne_name} annual report (2024 OR 2023) filetype:pdf {site_filter}"
    mne = {"NAME": mne_name, "ID": 0}
    annual_report = await ar_fetcher.async_fetch_for(mne, web_query=query)
    return annual_report


# Streamlit app
st.title("MNE Information and Annual Report Viewer")

mne_name_input = st.text_input("Enter the name of the Multinational Enterprise (MNE):")

if st.button("Extract Info") and mne_name_input:
    with st.spinner(f"Extracting financial information for {mne_name_input}..."):
        info_merged = asyncio.run(fetch_financial_info(mne_name_input))

        st.header("Extracted Financial Information")
        info_dict = {item.variable: (item.value, item.year) for item in info_merged}
        for var in ["COUNTRY", "EMPLOYEES", "TURNOVER", "ASSETS", "WEBSITE", "ACTIVITY"]:
            val, year = info_dict.get(var, ("Not available", "N/A"))
            st.write(f"**{var.capitalize()}:** {val} (Year: {year})")

    website_url = info_dict.get("WEBSITE", (None,))[0]

    if website_url:
        with st.spinner(f"Fetching annual report PDF for {mne_name_input}..."):
            annual_report = asyncio.run(fetch_annual_report_pdf(mne_name_input, website_url))

        if annual_report and annual_report.pdf_url:
            st.header("Annual Report Preview")
            st.markdown(f"[Download Annual Report]({annual_report.pdf_url})")
            # response = requests.get(annual_report.pdf_url)
            # st.pdf(response.content)
            st.components.v1.iframe(annual_report.pdf_url, height=600, scrolling=True)
        else:
            st.warning("Annual report not found.")
    else:
        st.warning("No website URL available to search for annual report.")
