import asyncio
import logging
from typing import Optional

from src.discovery.models import OtherSources

from .official_registers.country_resolver import get_country_for_mne
from .official_registers.factory import OfficialRegisterFetcherFactory

logger = logging.getLogger(__name__)


class OfficialRegisterFetcher:
    async def async_fetch_for(self, mne: dict) -> Optional[OtherSources]:
        country = await get_country_for_mne(mne["NAME"])
        if not country:
            logger.error(f"Could not resolve country for MNE: {mne['NAME']}")
            return None
        try:
            fetcher = OfficialRegisterFetcherFactory.get_fetcher(country)
            return await fetcher.async_fetch_for(mne)
        except ValueError:
            logger.error(f"No specific sources found for country: {country}")
            return None

    def fetch_for(self, mne: dict) -> Optional[OtherSources]:
        return asyncio.run(self.async_fetch_for(mne))
