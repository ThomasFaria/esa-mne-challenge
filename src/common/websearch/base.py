from abc import ABC, abstractmethod
from typing import Dict, List


class WebSearch(ABC):
    @abstractmethod
    def search(self, query: str) -> List[Dict]:
        pass
