import asyncio
import logging
import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import httpx
import iso4217parse
import mwparserfromhell
import pycountry
import tldextract
import wikipedia

from extractors.models import ExtractedInfo
from fetchers.models import OtherSources
from fetchers.wikipedia import WikipediaFetcher

logger = logging.getLogger(__name__)


@dataclass
class QuantifiedData:
    value: Optional[int]
    year: Optional[int]
    currency: Optional[str]


class WikipediaExtractor:
    """
    Extracts structured company information from Wikipedia using both the Wikipedia API and Wikidata.

    It attempts to collect:
    - Country (ISO2)
    - Website
    - Number of employees
    - Turnover
    - Total assets
    - Summary of the activity

    Sources are resolved in parallel, prioritized by freshness or data quality.
    """

    def __init__(self, fetcher: WikipediaFetcher, client: httpx.AsyncClient):
        self.wikidata = WikiDataExtractor(client)
        self.api = WikipediaAPIExtractor(client)
        self.fetcher = fetcher
        self.client = client
        self.min_valid_year = 2024

    async def extract_wikipedia_infos(self, mne: dict) -> Optional[List[ExtractedInfo]]:
        """
        Given an MNE, resolves the associated Wikipedia page and extracts variables of interest.

        Args:
            mne (dict): A dictionary representing a multinational enterprise.

        Returns:
            Optional[List[ExtractedInfo]]: List of extracted variables (country, assets, etc.) with metadata.
        """
        mne_name = mne.get("NAME")
        mne_id = mne.get("ID")

        try:
            title = await self.fetcher.get_wikipedia_name(mne_name)
        except Exception as e:
            logger.error(f"Failed to resolve Wikipedia title for {mne_name}: {e}")
            return None

        try:
            # Run all extractions concurrently
            tasks = [
                self.get_country(title),  # 0
                self.get_website(title),  # 1
                self.get_employees(title),  # 2
                self.get_turnover(title),  # 3
                self.get_assets(title),  # 4
                self.get_activity(title),  # 5
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Unpack results safely
            def get_result(i, fallback=None):
                return results[i] if not isinstance(results[i], Exception) else fallback

            country = get_result(0)
            website = get_result(1)
            employees = get_result(2, QuantifiedData(None, None, None))
            turnover = get_result(3, QuantifiedData(None, None, None))
            assets = get_result(4, QuantifiedData(None, None, None))
            activity = get_result(5)

            try:
                source_url = await self.fetcher.fetch_wikipedia_page(mne)
                page_url = source_url.url
            except Exception as e:
                logger.warning(f"Could not fetch Wikipedia page URL for {mne_name}: {e}")
                page_url = None

            variables = [
                ("COUNTRY", country, 2024, "N/A"),
                ("EMPLOYEES", employees.value, employees.year, employees.currency),
                ("TURNOVER", turnover.value, turnover.year, turnover.currency),
                ("ASSETS", assets.value, assets.year, assets.currency),
                ("WEBSITE", website, 2024, "N/A"),
                ("ACTIVITY", activity, 2024, "N/A"),
            ]

            return [
                ExtractedInfo(
                    mne_id=mne_id,
                    mne_name=mne_name,
                    variable=var,
                    source_url=page_url,
                    value=val,
                    currency=curr,
                    year=year,
                )
                for var, val, year, curr in variables
                if val is not None and year is not None
            ] or None

        except Exception as e:
            logger.exception(f"Failed to extract info for {mne_name}: {e}")
            return None

    async def _choose(self, title: str, method: str):
        wd_method = getattr(self.wikidata, method)
        api_method = getattr(self.api, method)

        wd_task = asyncio.create_task(wd_method(title))
        api_task = asyncio.create_task(api_method(title))
        wd_value, api_value = await asyncio.gather(wd_task, api_task, return_exceptions=True)

        # For QuantifiedData (Assets, Employees, Turnover), we want to keep the most recent value
        if isinstance(wd_value, QuantifiedData) and isinstance(api_value, QuantifiedData):
            if wd_value.year and api_value.year:
                return wd_value if wd_value.year >= api_value.year else api_value
            return wd_value if wd_value.year else api_value

        # For other types, we prefer the Wikidata value if available
        return wd_value or api_value

    async def get_country(self, title: str) -> Optional[str]:
        return await self._choose(title, "get_country")

    async def get_website(self, title: str) -> Optional[str]:
        raw_url = await self._choose(title, "get_website")
        if raw_url is None:
            return None
        try:
            response = await self.client.get(raw_url, follow_redirects=True)
            extracted = tldextract.extract(str(response.url))
            return f"{extracted.domain}.{extracted.suffix}" if extracted.domain and extracted.suffix else raw_url
        except Exception:
            return raw_url

    async def get_employees(self, title: str) -> QuantifiedData:
        return await self._choose(title, "get_employees")

    async def get_turnover(self, title: str) -> QuantifiedData:
        return await self._choose(title, "get_turnover")

    async def get_assets(self, title: str) -> QuantifiedData:
        return await self._choose(title, "get_assets")

    async def get_activity(self, title: str) -> Optional[str]:
        try:
            return wikipedia.page(title, auto_suggest=False).summary
        except Exception:
            return None

    async def async_extract_for(self, mne: dict) -> Tuple[Optional[List[ExtractedInfo]], Optional[List[OtherSources]]]:
        try:
            sources = await self.fetcher.async_fetch_for(mne)
            infos = await self.extract_wikipedia_infos(mne)
            return infos, sources
        except Exception as e:
            logger.error(f"Error in async_extract_for for MNE {mne.get('NAME')}: {e}")
            return None, None

    def extract_for(self, mne: dict) -> Tuple[Optional[List[ExtractedInfo]], Optional[List[OtherSources]]]:
        return asyncio.run(self.async_extract_for(mne))


class WikiDataExtractor:
    """
    Extracts structured company information from Wikidata.
    """

    def __init__(self, client: httpx.AsyncClient):
        self.client = client
        self.api_base = "https://www.wikidata.org/wiki/Special:EntityData"

    async def get_qid(self, title: str) -> Optional[str]:
        url = "https://en.wikipedia.org/w/api.php"
        params = {"action": "query", "titles": title, "prop": "pageprops", "format": "json"}
        resp = await self.client.get(url, params=params)
        pages = resp.json().get("query", {}).get("pages", {})
        return list(pages.values())[0].get("pageprops", {}).get("wikibase_item")

    async def get_claims(self, qid: str) -> dict:
        url = f"{self.api_base}/{qid}.json"
        resp = await self.client.get(url)
        return resp.json()["entities"][qid].get("claims", {})

    def _parse_time(self, claim: dict) -> Optional[int]:
        try:
            return int(claim["qualifiers"]["P585"][0]["datavalue"]["value"]["time"][1:5])
        except Exception:
            return None

    async def _latest_value(self, title: str, prop: str, currency: bool = False) -> QuantifiedData:
        claims = await self._get_claims_if_valid(title)
        if not claims or prop not in claims:
            return QuantifiedData(None, None, None)
        try:
            dated = [(self._parse_time(c), c) for c in claims[prop] if self._parse_time(c)]
            latest = max(dated, key=lambda x: x[0]) if dated else (None, claims[prop][0])
            amount = int(latest[1]["mainsnak"]["datavalue"]["value"]["amount"].replace("+", ""))
            currency_code = "N/A"
            if currency:
                unit_id = latest[1]["mainsnak"]["datavalue"]["value"]["unit"].split("/")[-1]
                currency_code = await self._currency_label(unit_id)
            return QuantifiedData(amount, latest[0], currency_code)
        except Exception:
            return QuantifiedData(None, None, None)

    async def _currency_label(self, unit_id: str) -> str:
        try:
            url = f"{self.api_base}/{unit_id}.json"
            resp = await self.client.get(url)
            return resp.json()["entities"][unit_id]["claims"]["P498"][0]["mainsnak"]["datavalue"]["value"]
        except Exception:
            return "N/A"

    async def _country_label(self, unit_id: str) -> Optional[str]:
        try:
            url = f"{self.api_base}/{unit_id}.json"
            resp = await self.client.get(url)
            return resp.json()["entities"][unit_id]["labels"]["en"]["value"]
        except Exception:
            return None

    async def _get_claims_if_valid(self, title: str) -> Optional[dict]:
        qid = await self.get_qid(title)
        if not qid:
            return None
        return await self.get_claims(qid)

    async def get_country(self, title: str) -> Optional[str]:
        claims = await self._get_claims_if_valid(title)
        if not claims or "P17" not in claims:
            return None
        country_id = claims["P17"][0]["mainsnak"]["datavalue"]["value"]["id"]
        return pycountry.countries.search_fuzzy(await self._country_label(country_id))[0].alpha_2

    async def get_website(self, title: str) -> Optional[str]:
        claims = await self._get_claims_if_valid(title)
        try:
            return claims["P856"][0]["mainsnak"]["datavalue"]["value"]
        except Exception:
            return None

    async def get_employees(self, title: str) -> QuantifiedData:
        return await self._latest_value(title, "P1128")

    async def get_turnover(self, title: str) -> QuantifiedData:
        return await self._latest_value(title, "P2139", currency=True)

    async def get_assets(self, title: str) -> QuantifiedData:
        return await self._latest_value(title, "P2403", currency=True)


class WikipediaAPIExtractor:
    """
    Extracts structured company information from Wikidata.
    """

    INFOBOX_KEYS = {
        "location": ["hq_location_country", "location_country", "location", "hq_city", "hq_location"],
        "num_employees": ["num_employees"],
        "revenue": ["revenue"],
        "assets": ["assets"],
        "website": ["website", "homepage"],
    }

    def __init__(self, client: httpx.AsyncClient):
        self.client = client
        self.api_url = "https://en.wikipedia.org/w/api.php"

    async def _get_wikitext(self, title: str) -> str:
        params = {"action": "parse", "page": title, "prop": "wikitext", "format": "json"}
        resp = await self.client.get(self.api_url, params=params)
        return resp.json()["parse"]["wikitext"]["*"]

    def _filter_templates(self, templates) -> list:
        skip_names = {"cite web", "cite news", "increase", "decrease", "gain", "down", "unbulleted list"}
        return [t for t in templates if t.name.lower().strip() not in skip_names]

    def _parse_infobox(self, wikitext: str) -> Dict[str, str]:
        wikicode = mwparserfromhell.parse(wikitext)
        templates = [t for t in wikicode.filter_templates() if t.name.lower().strip().startswith("infobox")]
        fields = {}
        for template in templates:
            for field, keys in self.INFOBOX_KEYS.items():
                for key in keys:
                    if template.has(key):
                        try:
                            value = template.get(key).value
                            nested = self._filter_templates(value.filter_templates())
                            value_str = value.strip_code().strip()

                            if field in {"revenue", "assets"} and nested:
                                if nested[0].name.lower().strip() == "nowrap":
                                    renested = mwparserfromhell.parse(str(nested[0].params[0])).filter_templates()
                                    renested = self._filter_templates(renested)
                                    if renested:
                                        fields[field] = (
                                            f"{renested[0].name} {renested[0].params[0]} {nested[0].params[0].value.strip_code().strip()}"
                                        )
                                elif nested[0].params:
                                    unit = (
                                        nested[0].params[1].value.strip_code().strip()
                                        if len(nested[0].params) > 1
                                        else ""
                                    )
                                    fields[field] = f"{nested[0].name} {nested[0].params[0]} {unit} {value_str}"
                            elif field in {"website", "location"} and nested:
                                fields[field] = str(nested[0].params[0])
                            elif field == "num_employees" and nested:
                                fields[field] = f"{nested[0].params[0]} {value_str}"
                            else:
                                fields[field] = value_str
                            break
                        except Exception:
                            continue
        return fields

    def _extract_year(self, text: Optional[str]) -> Optional[int]:
        if not text:
            return None
        match = re.search(r"(\d{4})", text)
        return int(match.group(1)) if match else None

    def _parse_numeric_value(self, value_str: Optional[str]) -> Optional[int]:
        if not value_str:
            return None
        value_str = value_str.replace(",", "").replace("&nbsp;", "").lower()

        if value_str.startswith("msek") or value_str.startswith("sekm"):
            match = re.search(r"(?:msek|sekm)\s+((?:\d[\d\s,]*\d|\d[\d\s]*)+)\s*(?:\((\d{4})\))?", value_str)
            if match:
                number = match.group(1).replace(" ", "").replace(",", "")
                value_str = f"SEK {'{:,}'.format(int(number))} million"

        multipliers = {
            "trillion": 1_000_000_000_000,
            "billion": 1_000_000_000,
            "million": 1_000_000,
            "thousand": 1_000,
        }
        match = re.search(r"(\d+\.?\d*)\s*(trillion|billion|million|thousand)?", value_str)
        if match:
            number = float(match.group(1))
            multiplier = multipliers.get(match.group(2), 1)
            return int(number * multiplier)
        return None

    def _extract_currency(self, text: Optional[str]) -> Optional[str]:
        if not text:
            return None
        normalized = (
            text.lower()
            .replace("jpyconvert", "JPY")
            .replace("yen", "JPY")
            .replace("msek", "SEK")
            .replace("sekm", "SEK")
        )
        try:
            return iso4217parse.parse(normalized)[0].alpha3
        except Exception:
            return None

    async def get_country(self, title: str) -> Optional[str]:
        wikitext = await self._get_wikitext(title)
        fields = self._parse_infobox(wikitext)
        loc = fields.get("location")
        return loc.split(",")[-1].strip().replace("U.S.", "US") if loc else None

    async def get_website(self, title: str) -> Optional[str]:
        wikitext = await self._get_wikitext(title)
        fields = self._parse_infobox(wikitext)
        return fields.get("website")

    async def get_employees(self, title: str) -> QuantifiedData:
        return await self._extract_numeric(title, "num_employees")

    async def get_turnover(self, title: str) -> QuantifiedData:
        return await self._extract_numeric(title, "revenue", extract_currency=True)

    async def get_assets(self, title: str) -> QuantifiedData:
        return await self._extract_numeric(title, "assets", extract_currency=True)

    async def _extract_numeric(self, title: str, field: str, extract_currency: bool = False) -> QuantifiedData:
        try:
            wikitext = await self._get_wikitext(title)
            fields = self._parse_infobox(wikitext)
            raw = fields.get(field)
            value = self._parse_numeric_value(raw)
            year = self._extract_year(raw)
            currency = self._extract_currency(raw) if extract_currency else "N/A"
            return QuantifiedData(value, year, currency)
        except Exception:
            return QuantifiedData(None, None, None)
