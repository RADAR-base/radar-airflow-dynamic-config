from typing import Any
from abc import ABC, abstractmethod


class Storage(ABC):
    @abstractmethod
    def save(self, key: str, data: Any) -> None:
        pass

    @abstractmethod
    def load(self, key: str) -> Any:
        pass

    @abstractmethod
    def init(self, **kwargs):
        pass