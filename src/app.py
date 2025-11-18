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
from streamlit_javascript import st_javascript

import config
from common.websearch.duckduckgo import DuckDuckGoSearch
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
        searcher=[DuckDuckGoSearch(max_results=6)],
        model=os.getenv("GENERATION_MODEL"),
        llm_client=llm_client,
    )
    # Extractors
    wiki_extractor = WikipediaExtractor(fetcher=wiki, client=client)
    yahoo_extractor = YahooExtractor(yahoo)
    pdf_extractor = PDFExtractor(client=client, llm_client=llm_client, model=os.getenv("GENERATION_MODEL"))

    classifier = NACEClassifier(
        llm_client=llm_client,
        model=os.getenv("GENERATION_MODEL"),
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


user_agent = st_javascript("navigator.userAgent")

# initialize services
services = init_services()
client = services["client"]
wiki_extractor = services["wiki_extractor"]
yahoo_extractor = services["yahoo_extractor"]
pdf_extractor = services["pdf_extractor"]
ar_fetcher = services["ar_fetcher"]
classifier = services["classifier"]
official_register = services["official_register"]


def define_styles():
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
        height: 150px;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        margin-bottom: 1rem;
    }
    .info-value {
        font-size: 22px;
        margin: 8px 0;
    }
    .info-meta {
        font-size: 12px;
        color: #ccc;
    }
    .info-desc {
        font-size: 12px;
        color: #aaa;
        margin-top: 4px;
        white-space: normal;
    }
    .disclaimer-box {
        background-color: #fff3cd;
        border: 1px solid #ffeaa7;
        border-radius: 5px;
        padding: 1rem;
        margin: 1rem 0;
        font-size: 14px;
        color: #856404;
    }
    .disclaimer-box strong {
        color: #b8860b;
    }
    .footer {
        margin-top: 3rem;
        padding: 2rem 0 1rem 0;
        border-top: 1px solid #e0e0e0;
        text-align: center;
        color: #666;
        font-size: 12px;
    }
    .footer-section {
        margin-bottom: 1rem;
    }
    </style>
    """,
        unsafe_allow_html=True,
    )


def render_header():
    """Render header with logo and app information"""

    header_html = """
    <div style="display: flex; justify-content: space-between; align-items: center;">
        <h1>Multinational Enterprise Explorer</h1>
        <a href="https://statistics-awards.eu/web-intelligence/" target="_blank">
            <img src="https://statistics-awards.eu/static/img/version_3_logo_main.svg" alt="ESA Logo" style="height:150px;">
        </a>
    </div>
    """

    st.markdown(header_html, unsafe_allow_html=True)


def render_footer():
    """Render footer with additional information"""
    st.markdown(
        f"""
    <div class="footer">
        <div class="footer-section">
            Developed by Team Toad for Eurostat MNE Discovery & Extraction Challenges
        </div>
        <div class="footer-section">
            Please use responsibly and verify all extracted information
        </div>
        <div class="footer-section">
            The LLM currently used for classification and web search is {os.getenv("GENERATION_MODEL")}
        </div>
    </div>
    """,
        unsafe_allow_html=True,
    )


def get_country_display(code):
    try:
        country = pycountry.countries.get(alpha_2=code)
        return f"{country.flag} {country.name}"
    except Exception:
        return code


def render_card(title, value, year, source):
    meta_block = f'<div class="info-meta">Year: {year if year else "N/A"}{f' | <a href="{source}" target="_blank" style="color:#aaa">Source</a>' if source else ""}</div>'
    return f"""
    <div class='info-card'>
        <div><strong>{title}</strong></div>
        <div class='info-value'>{value}</div>
        {meta_block}
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
    code = item.value
    graph = get_rdf_graph(f"{BASE_URL}/{code[1:]}")
    subj = next(graph.subjects(SKOS.notation, Literal(code[1:])), None)
    desc = extract_notes(graph, subj)["preferred_label"] if subj else ""
    desc_block = f'<div class="info-desc">{desc}</div>'
    return f"""
    <div class='info-card'>
        <div><strong>ACTIVITY</strong></div>
        <div class='info-value'>{code}</div>
        {desc_block}
    </div>
    """


def display_info_cards(info_merged):
    var_to_fn = {
        "COUNTRY": display_country,
        "EMPLOYEES": display_employee,
        "TURNOVER": display_turnover,
        "ASSETS": display_assets,
        "WEBSITE": display_website,
        "ACTIVITY": display_activity,
    }

    # Collect all items that exist
    available_items = []
    for var in var_to_fn:
        for item in info_merged:
            if item.variable == var:
                available_items.append((var, item))
                break

    # Display items in rows of 3
    for i in range(0, len(available_items), 3):
        cols = st.columns(3)
        row_items = available_items[i : i + 3]

        for j, (var, item) in enumerate(row_items):
            html = var_to_fn[var](item)
            cols[j].markdown(html, unsafe_allow_html=True)

        # Add empty columns if the row is not complete
        for j in range(len(row_items), 3):
            cols[j].empty()


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
    mne = {"NAME": mne_name.upper(), "ID": 0}
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
    st.markdown(
        """
    <div class="disclaimer-box">
        <strong>⚠️ Data Accuracy:</strong> Results provided by this application are automatically extracted and may contain errors. Please verify all information independently before making any business decisions or reusing the data.
    </div>
    """,
        unsafe_allow_html=True,
    )

    report = st.session_state.get(f"{mne_name}_annual_pdf")
    if report and report.pdf_url:
        st.header("Annual Report Preview")
        st.markdown(f"[Download Report]({report.pdf_url})")
        if user_agent and "Edg" in user_agent:
            st.markdown(
                """
            <div class="disclaimer-box">
                <strong>⚠️ Browser Compatibility:</strong> PDF preview functionality may not work properly in Microsoft Edge. For optimal experience, please use Mozilla Firefox or Google Chrome.
            </div>
            """,
                unsafe_allow_html=True,
            )
        components.iframe(report.pdf_url, height=600)
    else:
        st.info(st.session_state.get(f"{mne_name}_pdf_status"))


define_styles()

render_header()

mne_input = st.text_input("MNE Name:")
if st.button("Run Extraction") and mne_input:
    asyncio.run(orchestrate_workflow(mne_input))

render_footer()
