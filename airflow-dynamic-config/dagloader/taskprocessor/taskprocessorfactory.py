from dagloader.taskprocessor.missingdatataskprocessor import MissingDataTaskProcessor
from dagloader.taskprocessor.taskprocessor import TaskProcessor


class TaskProcessorFactory:
    @staticmethod
    def get_task_processor(processor_type: str, **kwargs) -> TaskProcessor:
        if processor_type == 'data_checks':
            return MissingDataTaskProcessor(**kwargs)
        else:
            raise ValueError(f"Unsupported processor type: {processor_type}")
