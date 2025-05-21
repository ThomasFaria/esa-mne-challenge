import logging
import random
from typing import Optional

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
    """
    A fetcher class that queries the French government's 'Annuaire des Entreprises' API
    to retrieve structured company information (e.g., SIREN, activity) for a given MNE.
    """

    def __init__(self):
        """
        Initialize the base URL for the API endpoint.
        """
        self.URL_BASE = "https://recherche-entreprises.api.gouv.fr/search"

    async def fetch_page(self, mne: dict) -> Optional[OtherSources]:
        """
        Fetches the official company page from Annuaire des Entreprises using the SIREN identifier.

        Args:
            mne (dict): A dictionary containing at least "ID" and "NAME" for the multinational.

        Returns:
            Optional[OtherSources]: Structured source metadata if a match is found, otherwise None.
        """
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
        """
        Cleans the MNE name by removing formatting artifacts (e.g., "S A").

        Args:
            mne (dict): MNE dictionary with a "NAME" field.

        Returns:
            str: A cleaned company name suitable for query.
        """
        name = mne["NAME"].replace("S A", "").strip()
        return name

    async def async_fetch_for(self, mne: dict) -> Optional[OtherSources]:
        """
        Async wrapper for fetching company data from the official Annuaire des Entreprises.

        Args:
            mne (dict): MNE dictionary.

        Returns:
            Optional[OtherSources]: Resulting company source data or None.
        """
        return await self.fetch_page(mne)
