import abc
from abc import ABC, abstractmethod


class DataReader(ABC):
    @abstractmethod
    def get_reader_task(self):
        pass