from abc import ABC, abstractmethod


class TaskProcessor(ABC):

    def __init__(self):
        pass

    @abstractmethod
    def get_processor_task(self, **kwargs):
        pass