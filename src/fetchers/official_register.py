import asyncio
import logging
from typing import Optional

from .models import OtherSources
from .official_registers.factory import OfficialRegisterFetcherFactory

logger = logging.getLogger(__name__)


class OfficialRegisterFetcher:
    async def async_fetch_for(self, mne: dict, country: str) -> Optional[OtherSources]:
        try:
            fetcher = OfficialRegisterFetcherFactory.get_fetcher(country)
            return await fetcher.async_fetch_for(mne)
        except ValueError:
            logger.debug(f"No specific sources found for country: {country}")
            return None

    def fetch_for(self, mne: dict, country: str) -> Optional[OtherSources]:
        return asyncio.run(self.async_fetch_for(mne, country))
