import asyncio
import os

import httpx
import nest_asyncio
import pycountry
import streamlit as st
import streamlit.components.v1 as components
from currency_symbols import CurrencySymbols
from langfuse.openai import AsyncOpenAI
from millify import millify, prettify
from rdflib import Literal
from rdflib.namespace import SKOS

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
from vector_db.notices_nace import BASE_URL, extract_notes, get_rdf_graph

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


def get_country_display(code):
    try:
        country = pycountry.countries.get(alpha_2=code)
        flag = country.flag
        return f"{flag} {country.name}"
    except Exception:
        return code


def render_card(title, value, year, source):
    return f"""
    <div class='info-card'>
        <div><strong>{title}</strong></div>
        <div style='font-size: 22px; margin-top: 8px; margin-bottom: 8px'>{value}</div>
        <div style='font-size: 12px; color: #ccc'>Year: {year if year else "N/A"}{f' | <a href="{source}" target="_blank" style="color:#aaa">Source</a>' if source else ""}</div>
    </div>
    """


def display_country(item):
    return render_card("COUNTRY", get_country_display(item.value), item.year, item.source_url)


def display_employee(item):
    return render_card("EMPLOYEES", prettify(item.value), item.year, item.source_url)


def display_turnover(item):
    formatted = (
        f"{millify(item.value, precision=3)} {CurrencySymbols.get_symbol(item.currency)}"
        if hasattr(item, "currency")
        else millify(item.value, precision=3)
    )
    return render_card("TURNOVER", formatted, item.year, item.source_url)


def display_assets(item):
    formatted = (
        f"{millify(item.value, precision=3)} {CurrencySymbols.get_symbol(item.currency)}"
        if hasattr(item, "currency")
        else millify(item.value, precision=3)
    )
    return render_card("ASSETS", formatted, item.year, item.source_url)


def display_website(item):
    return render_card(
        "WEBSITE", f"<a href='https://{item.value}' target='_blank'>{item.value}</a>", item.year, item.source_url
    )


def display_activity(item):
    title = "ACTIVITY"
    value = item.value
    graph = get_rdf_graph(f"{BASE_URL}/{value[1:]}")
    subj = next(graph.subjects(SKOS.notation, Literal(value[1:])))
    description = extract_notes(graph, subj)["preferred_label"]
    html = f"""
    <div class='info-card'>
        <div><strong>{title}</strong></div>
        <div style='font-size: 22px; margin-top: 8px; margin-bottom: 8px'>{value}</div>
        <div style='font-size: 12px; color: #ccc'>{description}</div>
    </div>
    """
    return html


def display_info_cards(info_merged):
    var_to_fn = {
        "COUNTRY": display_country,
        "EMPLOYEES": display_employee,
        "TURNOVER": display_turnover,
        "ASSETS": display_assets,
        "WEBSITE": display_website,
        "ACTIVITY": display_activity,
    }
    st.markdown(
        """
    <style>
    .info-card {
        background-color: #343a40;
        color: white;
        padding: 20px;
        border-radius: 10px;
        text-align: center;
        font-size: 16px;
        height: 100%;
    }
    </style>
    """,
        unsafe_allow_html=True,
    )

    cols = st.columns(3)
    idx = 0
    for var in var_to_fn:
        for item in info_merged:
            if item.variable == var:
                html = var_to_fn[var](item)
                cols[idx % 3].markdown(html, unsafe_allow_html=True)
                idx += 1
                break


async def extract_initial_info(mne):
    with st.spinner("Extracting initial information from Yahoo and Wikipedia..."):
        yahoo_res, wiki_res = await asyncio.gather(
            yahoo_extractor.async_extract_for(mne),
            wiki_extractor.async_extract_for(mne),
        )
    return merge_extracted_infos(yahoo_res[0], wiki_res[0])


async def classify_activity(activity_desc, country, mne_name, mne):
    st.session_state[f"{mne_name}_activity_status"] = "Classifying..."
    with st.spinner("Classifying the activity using official registers or LLM classifier..."):
        country_spec = await official_register.async_fetch_for(mne, country=country)
        if country_spec and country_spec.mne_activity:
            code = f"{classifier.mapping[country_spec.mne_activity]}{country_spec.mne_activity}"
        else:
            code = (await classifier.classify(activity_desc)).code
        st.session_state[f"{mne_name}_classified_activity"] = code
        st.session_state[f"{mne_name}_activity_status"] = "Done"


async def fetch_annual_report(mne, mne_name, website_url):
    st.session_state[f"{mne_name}_pdf_status"] = "Searching report..."
    with st.spinner("Fetching annual report from the web..."):
        query = f"{mne_name} annual report (2024 OR 2023) filetype:pdf {f'site:{website_url}' if website_url else ''}"
        report = await ar_fetcher.async_fetch_for(mne, web_query=query)
        if report and report.pdf_url:
            st.session_state[f"{mne_name}_annual_pdf"] = report
            st.session_state[f"{mne_name}_pdf_status"] = "Found"
        else:
            st.session_state[f"{mne_name}_annual_pdf"] = None
            st.session_state[f"{mne_name}_pdf_status"] = "Not found"


async def orchestrate_workflow(mne_name):
    mne = {"NAME": mne_name, "ID": 0}
    info_merged = await extract_initial_info(mne)

    activity_desc = next((i.value for i in info_merged if i.variable == "ACTIVITY"), None)
    website_url = next((i.value for i in info_merged if i.variable == "WEBSITE"), None)
    country = next((i.value for i in info_merged if i.variable == "COUNTRY"), None)
    tasks = []
    if activity_desc:
        tasks.append(classify_activity(activity_desc, country, mne_name, mne))
    if website_url:
        tasks.append(fetch_annual_report(mne, mne_name, website_url))
    if tasks:
        with st.spinner("Running classification and fetching the annual report..."):
            await asyncio.gather(*tasks)

    missing = [
        v
        for v in ["COUNTRY", "EMPLOYEES", "TURNOVER", "ASSETS", "WEBSITE", "ACTIVITY"]
        if not any(i.variable == v and (i.year or 0) >= 2023 for i in info_merged)
    ]
    report = st.session_state.get(f"{mne_name}_annual_pdf")
    if missing and report and report.year >= 2024:
        with st.spinner(f"Extracting {missing} from PDF... this may take a moment."):
            pdf_infos = await pdf_extractor.async_extract_for(report.pdf_url, missing)
            info_merged.extend(pdf_extractor.extend_missing_vars(pdf_infos, mne, report, missing))
            info_merged = deduplicate_by_latest_year(info_merged)
            st.success("PDF extraction complete.")
    elif missing:
        st.warning("Some variables are missing or outdated, but no recent report available.")

    for item in info_merged:
        if item.variable == "ACTIVITY":
            item.value = st.session_state.get(f"{mne_name}_classified_activity")

    display_info_cards(info_merged)
    report = st.session_state.get(f"{mne_name}_annual_pdf")
    if report and report.pdf_url:
        st.header("Annual Report Preview")
        st.markdown(f"[Download Report]({report.pdf_url})")
        components.iframe(report.pdf_url, height=600)
    else:
        st.info(st.session_state.get(f"{mne_name}_pdf_status"))


st.title("MNE Info Explorer")
mne_input = st.text_input("MNE Name:")
if st.button("Run Extraction") and mne_input:
    asyncio.run(orchestrate_workflow(mne_input))
