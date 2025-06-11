from abc import ABC, abstractmethod
from typing import Optional, Tuple


class BaseExtractor(ABC):
    @abstractmethod
    async def get_country(self, title: str) -> Optional[str]:
        pass

    @abstractmethod
    async def get_website(self, title: str) -> Optional[str]:
        pass

    @abstractmethod
    async def get_employees(self, title: str) -> Tuple[Optional[int], Optional[int], Optional[str]]:
        pass

    @abstractmethod
    async def get_turnover(self, title: str) -> Tuple[Optional[int], Optional[int], Optional[str]]:
        pass

    @abstractmethod
    async def get_assets(self, title: str) -> Tuple[Optional[int], Optional[int], Optional[str]]:
        pass
