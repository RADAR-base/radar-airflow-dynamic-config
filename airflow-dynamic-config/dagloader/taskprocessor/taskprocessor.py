from abc import ABC, abstractmethod
from typing import Any, Dict


class TaskProcessor(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def execute(self, data: Dict[str, Any], **kwargs):
        pass
