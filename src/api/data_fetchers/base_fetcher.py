from abc import ABC, abstractmethod
from typing import List, Dict, Any


class BaseFetcher(ABC):
    @abstractmethod
    def fetch(self, data_id: str = None) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def to_csv(self, data: List[Dict[str, Any]]) -> str:
        pass
