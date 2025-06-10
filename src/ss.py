import asyncio
import logging
import os
import re

import iso4217parse
import mwparserfromhell
import pycountry
import requests

import config
from common.data import load_mnes
from common.paths import DATA_DISCOVERY_PATH
from common.websearch.google import GoogleSearch
from extraction.wikipedia import WikipediaExtractor
from fetchers.annual_reports import AnnualReportFetcher
from fetchers.official_register import OfficialRegisterFetcher
from fetchers.wikipedia import WikipediaFetcher
from fetchers.yahoo import YahooFetcher


def get_wikitext(title):
    endpoint = "https://en.wikipedia.org/w/api.php"
    params = {
        "action": "parse",
        "page": title,
        "prop": "wikitext|text|extracts",
        "format": "json",
    }
    response = requests.get(endpoint, params=params)
    response.raise_for_status()
    return response.json()["parse"]


def parse_infobox(wikitext):
    wikicode = mwparserfromhell.parse(wikitext)
    templates = wikicode.filter_templates()
    fields = {}
    keys = {
        "location": ["hq_location_country", "location_country", "location", "hq_city", "hq_location"],
        "num_employees": ["num_employees"],
        "revenue": ["revenue"],
        "assets": ["assets"],
        "website": ["website", "homepage"],
    }
    for template in templates:
        if template.name.lower().strip().startswith("infobox"):
            for k, v in keys.items():
                for key in v:
                    if template.has(key):
                        try:
                            nested_template = template.get(key).value.filter_templates()
                            nested_template = [
                                t
                                for t in nested_template
                                if t.name.lower().strip() not in ["cite web", "increase", "gain", "decrease", "down"]
                            ]
                            if nested_template and k in ["revenue", "assets"]:
                                if nested_template[0].name.lower().strip() == "nowrap":
                                    value_str = f"{nested_template[1].name} {nested_template[1].params[0]} {template.get(key).value.strip_code().strip()}"
                                else:
                                    unit = None
                                    if len(nested_template[0].params) > 1:
                                        unit = nested_template[0].params[1].value.strip_code().strip()
                                        unit = unit.replace("t", "trillion")
                                    value_str = f"{nested_template[0].name} {nested_template[0].params[0]} {unit if unit else ''} {template.get(key).value.strip_code().strip()}"
                            elif nested_template and k in ["website"]:
                                value_str = nested_template[0].params[0]
                            elif nested_template and k in ["num_employees"]:
                                value_str = (
                                    f"{nested_template[0].params[0]} {template.get(key).value.strip_code().strip()}"
                                )
                            else:
                                value_str = template.get(key).value.strip_code().strip()
                            fields[k] = value_str
                            break
                        except Exception:
                            # If there's an error, we just skip this key
                            continue
    return fields


def get_stripped_value(template, key):
    param = template.get(key)
    if param is not None:
        # Check if the parameter is a string or a Wikicode object
        if isinstance(param, str):
            # If it's a string, strip any wiki markup
            stripped_value = mwparserfromhell.parse(param).strip_code()
        else:
            # If it's a Wikicode object, use its value and strip the code
            stripped_value = param.value.strip_code()
        return stripped_value
    return None


def harmonize_country(country_str):
    if not country_str:
        return None
    return country_str.split(",")[-1].strip()


def extract_year(text):
    if not text:
        return None
    match = re.search(r"(\d{4})", text)
    return int(match.group(1)) if match else None


def parse_numeric_value(value_str):
    if not value_str:
        return None

    if value_str.startswith("MSEK"):
        pattern = r"MSEK\s+((?:\d[\d\s]*)+)\s*\((\d{4})\)"
        match = re.search(pattern, value_str)
        number = match.group(1).replace(" ", "")
        value_str = f"SEK {'{:,}'.format(int(number))} million"

    multipliers = {"trillion": 1_000_000_000_000, "billion": 1_000_000_000, "million": 1_000_000, "thousand": 1_000}
    match = re.search(
        r"(\d+\.?\d*)\s*(trillion|billion|million|thousand)?", value_str.lower().replace(",", "").replace("&nbsp;", "")
    )

    if match:
        number = float(match.group(1))
        multiplier = multipliers.get(match.group(2), 1)
        return int(number * multiplier)

    return None


def extract_currency(text):
    if not text:
        return None
    text = text.replace("JPYConvert", "JPY").replace("yen", "JPY").replace("MSEK", "SEK")
    return iso4217parse.parse(text)[0].alpha3


os.chdir("esa-mne-challenge/src")


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


for mne in mnes:
    mne_name = mne.get("NAME")
    mne_id = mne.get("ID")
    print({mne_name})
    # Get the wikipedia name of the MNE
    wiki_name = asyncio.run(wiki_extractor.fetcher.get_wikipedia_name(mne_name))

    page_data = get_wikitext(wiki_name)
    wikitext = page_data["wikitext"]["*"]

    infobox_fields = parse_infobox(wikitext)

    country = harmonize_country(infobox_fields.get("location"))
    try:
        country_iso2 = pycountry.countries.search_fuzzy(country)[0].alpha_2
    except (LookupError, AttributeError):
        country_iso2 = None

    employees_raw = infobox_fields.get("num_employees")
    employees = parse_numeric_value(employees_raw)
    employees_year = extract_year(employees_raw)

    revenue_raw = infobox_fields.get("revenue")
    revenue = parse_numeric_value(revenue_raw)
    revenue_year = extract_year(revenue_raw)
    revenue_currency = extract_currency(revenue_raw)

    assets_raw = infobox_fields.get("assets")
    assets = parse_numeric_value(assets_raw)
    assets_year = extract_year(assets_raw)
    assets_currency = extract_currency(assets_raw)

    website = infobox_fields.get("website")

    print(
        f"Country : {country_iso2}, Employees: {employees} -> year: {employees_year}, Revenue: {revenue}: currency -> {revenue_currency} -> year: {revenue_year}, Assets: {assets} -> currency: {assets_currency} -> year: {assets_year}, Website: {website}"
    )
