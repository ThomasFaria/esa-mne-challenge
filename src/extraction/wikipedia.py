import asyncio
import logging
from typing import List, Optional, Tuple

import pycountry
import requests
import wikipedia

from extraction.models import ExtractedInfo
from fetchers.models import OtherSources
from fetchers.wikipedia import WikipediaFetcher

logger = logging.getLogger(__name__)


class WikipediaExtractor:
    """
    A class to extract informations from Wikipedia page for MNEs.
    """

    def __init__(self, fetcher: Optional[WikipediaFetcher] = None):
        """
        Initialize the WikipediaExtractor with the fetcher.
        """
        self.fetcher = fetcher
        self.URL_BASE = "https://en.wikipedia.org/w/api.php"
        self.URL_WIKI_DATA = "https://www.wikidata.org/wiki/Special:EntityData"

    async def _get_qid(self, wiki_title: str) -> Optional[str]:
        params = {"action": "query", "titles": wiki_title, "prop": "pageprops", "format": "json"}
        resp = requests.get(self.URL_BASE, params=params).json()
        pages = resp.get("query", {}).get("pages", {})
        return list(pages.values())[0].get("pageprops", {}).get("wikibase_item")

    def _get_claim_value(
        self,
        claims: dict,
        prop: str,
        value_type: str = "string",
        currency: bool = False,
    ) -> Optional[str]:
        try:
            if value_type == "id":
                return claims[prop][0]["mainsnak"]["datavalue"]["value"]["id"]

            elif value_type == "amount":
                dated_claims = []
                for claim in claims[prop]:
                    date = self._parse_claim_time(claim)
                    if date:
                        dated_claims.append((date, claim))

                # Get the most recent claim
                latest_date, latest_claim = (
                    max(dated_claims, key=lambda x: x[0]) if dated_claims else (None, claims[prop][0])
                )
                amount = int(latest_claim["mainsnak"]["datavalue"]["value"]["amount"].replace("+", ""))
                if currency:
                    unit_id = latest_claim["mainsnak"]["datavalue"]["value"].get("unit").split("/")[-1]
                    unit_label = self._wiki_id_to_label(unit_id, currency)
                else:
                    unit_label = "N/A"
                return amount, latest_date, unit_label

            elif value_type == "string":
                return claims[prop][0]["mainsnak"]["datavalue"]["value"]
        except (KeyError, IndexError, TypeError):
            return (None, None, None) if value_type == "amount" else None

    async def _get_claims(self, qid: str) -> Optional[dict]:
        wikidata_url = f"{self.URL_WIKI_DATA}/{qid}.json"
        data = requests.get(wikidata_url).json()
        return data["entities"][qid].get("claims", {})

    def _parse_claim_time(self, claim: dict) -> Optional[int]:
        try:
            raw_time = claim["qualifiers"]["P585"][0]["datavalue"]["value"]["time"]
            return int(raw_time[1:5])  # Extract the year from the time string
        except Exception:
            return None

    def _wiki_id_to_label(
        self,
        wiki_id: str,
        currency: bool = False,
    ) -> Optional[str]:
        """
        Convert a Wikidata ID to its label.
        """
        url = f"{self.URL_WIKI_DATA}/{wiki_id}.json"
        try:
            response = requests.get(url)
            data = response.json()
            if currency:
                return data["entities"][wiki_id]["claims"]["P498"][0]["mainsnak"]["datavalue"]["value"]
            else:
                return data["entities"][wiki_id]["labels"]["en"]["value"]
        except (KeyError, requests.RequestException):
            return None

    async def extract_wikipedia_infos(self, mne: dict) -> Optional[ExtractedInfo]:
        """
        Extract informations from Wikipedia for a given MNE.

        Args:
            mne (dict): MNE metadata.

        Returns:
            Optional[ExtractedInfo]: Extracted information or None.
        """
        mne_name = mne.get("NAME")
        mne_id = mne.get("ID")

        # Get the wikipedia name of the MNE
        wiki_name = await self.fetcher.get_wikipedia_name(mne_name)
        wiki_page = wikipedia.page(wiki_name, auto_suggest=False)

        # Get the QID for the Wikipedia page
        qid = await self._get_qid(wiki_name)

        if not qid:
            return None

        claims = await self._get_claims(qid)

        try:
            tasks = [
                self.get_country(claims),  # 0
                self.get_website(claims),  # 1
                self.get_employees(claims),  # 2
                self.get_turnover(claims),  # 3
                self.get_assets(claims),  # 4
                self.get_activity(wiki_page),  # 5
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Handle exceptions in results
            infos = []
            for idx, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"Error in task {idx} for MNE {mne_name}: {result}")
                    infos.append(None) if idx in [0, 1, 5] else infos.append((None, None, None))
                else:
                    infos.append(result)

            country, website, employees, turnover, assets, activity = infos

            variables = [
                ("COUNTRY", country, 2024, "N/A"),
                ("EMPLOYEES", employees[0], employees[1], employees[2]),
                ("TURNOVER", turnover[0], turnover[1], turnover[2]),
                ("ASSETS", assets[0], assets[1], assets[2]),
                ("WEBSITE", website, 2024, "N/A"),
                ("ACTIVITY", activity, 2024, "N/A"),
            ]

            source_url = await self.fetcher.fetch_wikipedia_page(mne)

            results = [
                ExtractedInfo(
                    mne_id=mne_id,
                    mne_name=mne_name,
                    variable=var,
                    source_url=source_url.url,
                    value=val,
                    currency=curr,
                    year=year,
                )
                for var, val, year, curr in variables
                if val is not None and year is not None
            ]
            return results if results else None

        except Exception as e:
            logger.exception(f"Unhandled error extracting info for {mne_name}: {e}")
            return None

    async def get_country(self, claims: dict) -> str:
        country_id = self._get_claim_value(claims, "P17", "id")

        if not country_id:
            return None

        # Resolve the country label
        label = self._wiki_id_to_label(country_id)

        # Convert to ISO2
        country_iso2 = pycountry.countries.search_fuzzy(label)[0].alpha_2
        return country_iso2

    async def get_website(self, claims: dict) -> str:
        return self._get_claim_value(claims, "P856", "string")

    async def get_employees(self, claims: dict) -> tuple[int, int]:
        return self._get_claim_value(claims, "P1128", "amount")

    async def get_turnover(self, claims: dict) -> tuple[int, int]:
        return self._get_claim_value(claims, "P2139", "amount", currency=True)

    async def get_assets(self, claims: dict) -> tuple[int, int]:
        return self._get_claim_value(claims, "P2403", "amount", currency=True)

    async def get_activity(self, wiki_page) -> str:
        return wiki_page.summary

    async def async_extract_for(self, mne: dict) -> Tuple[Optional[List[ExtractedInfo]], Optional[List[OtherSources]]]:
        """
        Async wrapper to extract informations from Wikipedia page for a given MNE.

        Args:
            mne (dict): MNE metadata.

        Returns:
            Tuple[Optional[List[ExtractedInfo]], Optional[List[OtherSources]]]: Tuple of extracted wikipedia information and sources, or None.
        """
        sources = await self.fetcher.async_fetch_for(mne)
        infos = await self.extract_wikipedia_infos(mne)
        return infos, sources

    def extract_for(self, mne: dict) -> Tuple[Optional[List[ExtractedInfo]], Optional[List[OtherSources]]]:
        """
        Sync wrapper around the async Wikipedia Extractor.

        Args:
            mne (dict): MNE metadata.

        Returns:
            Tuple[Optional[List[ExtractedInfo]], Optional[List[OtherSources]]]: Tuple of extracted wikipedia information and sources, or None.
        """
        return asyncio.run(self.async_extract_for(mne))
