import asyncio
from typing import Optional

import wikipedia

from src.discovery.models import OtherSources
from src.discovery.utils import clean_mne_name


class WikipediaFetcher:
    """
    A class to fetch Wikipedia pages for MNEs.
    """

    def __init__(self):
        """
        Initialize the WikipediaFetcher.
        """
        wikipedia.set_lang("en")

    async def fetch_wikipedia_page(self, mne: dict) -> OtherSources:
        """
        Fetch the Wikipedia page for a given MNE.
        """

        # Clean the MNE name
        mne_short = clean_mne_name(mne["NAME"])

        # Add "(group)" for certain MNEs with ambiguous names
        if mne["NAME"] in ["FCC", "ETEX", "THALES", "CANON INCORPORATED", "EDIZIONE", "FERRERO"]:
            mne_short = f"{mne_short} (group)"

        wiki_search = wikipedia.search(f"{mne_short}")
        wiki_name = wiki_search[0]

        try:
            wiki_page = wikipedia.page(wiki_name, auto_suggest=False)
            wiki_url = wiki_page.url
        except wikipedia.exceptions.PageError:
            wiki_url = f"https://en.wikipedia.org/wiki/{wiki_name.replace(' ', '_')}"
        except wikipedia.exceptions.DisambiguationError:
            wiki_url = f"https://en.wikipedia.org/wiki/{wiki_name}"

        # We always specify the year as 2024 but will make it consistent with the year of the report retrieved
        return OtherSources(mne_id=mne["ID"], mne_name=mne["NAME"], source_name="Wikipedia", url=wiki_url, year=2024)

    async def async_fetch_for(self, mne: dict) -> Optional[OtherSources]:
        return await self.fetch_wikipedia_page(mne)

    def fetch_for(self, mne: dict) -> Optional[OtherSources]:
        return asyncio.run(self.fetch_wikipedia_page(mne))
