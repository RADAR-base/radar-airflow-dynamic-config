from abc import ABC, abstractmethod


class TaskProcessor(ABC):
    @abstractmethod
    def get_processor_task(self, **kwargs):
        pass

    @abstractmethod
    def get_data_processor_task(self, **kwargs):
        pass