import logging
import random

import requests

from src.discovery.models import OtherSources

logger = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Mozilla/5.0 (X11; Linux x86_64)",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:112.0) Gecko/20100101 Firefox/112.0",
]


class AnnuaireEntrepriseFetcher:
    def __init__(self):
        self.URL_BASE = "https://recherche-entreprises.api.gouv.fr/search"

    async def fetch_page(self, mne: dict) -> OtherSources:
        headers = {"User-Agent": random.choice(USER_AGENTS)}
        mne_cleaned = self.clean_mne_name(mne)
        params = {"q": mne_cleaned, "categorie_entreprise": "GE"}
        response = requests.get(self.URL_BASE, headers=headers, params=params)
        if response.status_code != 200:
            logger.error(f"Failed to fetch for {mne['NAME']}: {response.status_code}")
            return None

        try:
            data = response.json()["results"][0]
            siren = data["siren"]
            url = f"https://annuaire-entreprises.data.gouv.fr/entreprise/{siren}"
            status = requests.head(url, headers=headers).status_code

            if status == 200:
                return OtherSources(
                    mne_id=mne["ID"],
                    mne_name=mne["NAME"],
                    source_name="Annuaire Entreprise",
                    url=url,
                    year="2024",
                    mne_national_id=siren,
                    mne_activity=data["siege"]["activite_principale"],
                )
        except (IndexError, KeyError):
            logger.error(f"Unexpected data format for {mne['NAME']}: {response.text}")
            return None

    def clean_mne_name(self, mne: dict) -> str:
        name = mne["NAME"].replace("S A", "").strip()
        return name

    async def async_fetch_for(self, mne: dict):
        return await self.fetch_page(mne)
