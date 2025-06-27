"""
Abstract base class for implementing custom search engines that return
search results in a standardized list-of-dicts format.
"""

from abc import ABC, abstractmethod
from typing import Dict, List


class WebSearch(ABC):
    @abstractmethod
    def search(self, query: str) -> List[Dict]:
        pass
