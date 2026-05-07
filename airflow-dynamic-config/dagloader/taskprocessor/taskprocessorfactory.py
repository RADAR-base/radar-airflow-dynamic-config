from dagloader.taskprocessor.missingdatataskprocessor import MissingDataTaskProcessor
from dagloader.taskprocessor.taskprocessor import TaskProcessor
from dagloader.taskprocessor.customtaskprocessor import CustomTaskProcessor
import logging
logger = logging.getLogger(__name__)

class TaskProcessorFactory:
    @staticmethod
    def get_task_processor(processor_type: str, **kwargs) -> TaskProcessor:
        if processor_type == 'data_checks':
            return MissingDataTaskProcessor(intermediate_storage=kwargs.get('intermediate_storage'))
        if processor_type == 'custom':
            logger.info(f"Creating CustomTaskProcessor with kwargs: {kwargs}")
            return CustomTaskProcessor(intermediate_storage=kwargs.get('intermediate_storage'),
                                       path=kwargs.get('task_config', {}).get('path'))
        else:
            raise ValueError(f"Unsupported processor type: {processor_type}")
