import asyncio
from typing import Optional

import wikipedia

from .models import OtherSources
from .utils import clean_mne_name


class WikipediaFetcher:
    """
    Fetches the Wikipedia page URL for a given multinational enterprise (MNE).

    Uses the Wikipedia Python API to search for the most relevant page based on the MNE name.
    """

    def __init__(self):
        """
        Initialize the WikipediaFetcher with English as the default language.
        """
        wikipedia.set_lang("en")

    async def get_wikipedia_name(self, mne_name: str) -> str:
        """
        Deduce the Wikipedia page title for a given MNE name using the Wikipedia API.

        Args:
            mne_name (str): The full name of the MNE.

        Returns:
            str: The best-matching Wikipedia page title.
        """

        # Clean the MNE name
        mne_short = clean_mne_name(mne_name)

        # Add "(group)" for certain MNEs with ambiguous names
        if mne_name in [
            "FCC",
            "ETEX",
            "THALES",
            "CANON INCORPORATED",
            "EDIZIONE",
            "FERRERO",
            "ORANGE",
            "CRH PLC",
            "TUI",
            "EDP",
            "SGS",
            "SOLVAY",
            "VINCI",
        ]:
            mne_short = f"{mne_short} (group)"

        # Add "(company)" for certain MNEs with ambiguous names
        if mne_name in ["AMAZON"]:
            mne_short = f"{mne_short} (company)"

        wiki_search = wikipedia.search(f"{mne_short}")
        wiki_name = wiki_search[0]
        return wiki_name

    async def fetch_wikipedia_page(self, mne: dict) -> OtherSources:
        """
        Retrieve the Wikipedia page URL for the given MNE.

        Performs a Wikipedia search and constructs a valid URL from the page titles.

        Args:
            mne (dict): Dictionary with keys "ID" and "NAME".

        Returns:
            OtherSources: An object containing the Wikipedia source URL and metadata.
        """

        # try:
        wiki_name = await self.get_wikipedia_name(mne["NAME"])
        wiki_page = wikipedia.page(wiki_name, auto_suggest=False)
        wiki_url = wiki_page.url
        # except wikipedia.exceptions.PageError:
        #     wiki_url = f"https://en.wikipedia.org/wiki/{wiki_name.replace(' ', '_')}"
        # except wikipedia.exceptions.DisambiguationError:
        #     wiki_url = f"https://en.wikipedia.org/wiki/{wiki_name}"
        # except wikipedia.exceptions.WikipediaException:
        #     wiki_url = f"https://en.wikipedia.org/wiki/{mne['NAME']}"

        # We always specify the year as 2024 but will make it consistent with the year of the report retrieved
        return OtherSources(mne_id=mne["ID"], mne_name=mne["NAME"], source_name="Wikipedia", url=wiki_url, year=2024)

    async def async_fetch_for(self, mne: dict) -> Optional[OtherSources]:
        """
        Async wrapper for fetching the Wikipedia page for an MNE.

        Args:
            mne (dict): MNE metadata.

        Returns:
            Optional[OtherSources]: Wikipedia URL or None.
        """
        return await self.fetch_wikipedia_page(mne)

    def fetch_for(self, mne: dict) -> Optional[OtherSources]:
        """
        Synchronous wrapper for async Wikipedia page fetch.

        Args:
            mne (dict): MNE metadata.

        Returns:
            Optional[OtherSources]: Wikipedia URL or None.
        """
        return asyncio.run(self.async_fetch_for(mne))
