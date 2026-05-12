from airflow.sdk import BaseOperator
from dagloader.taskprocessor.taskprocessorfactory import TaskProcessorFactory
import logging
logger = logging.getLogger(__name__)

class TaskOperator(BaseOperator):
    def __init__(self, task_config, intermediate_storage, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.intermediate_storage = intermediate_storage
        self.task_config = task_config
        self.task_processor = TaskProcessorFactory.get_task_processor(
            processor_type=self.task_config.get('type', 'missing_data'),
            intermediate_storage=self.intermediate_storage,
            task_config = self.task_config
        )
        self.task_data_sources = self.task_config.get('data_sources', [])

    def execute(self, context):
        # Update task_config from runtime parameters if available
        runtime_tasks = context['params'].get('tasks', [])
        runtime_config = next((t for t in runtime_tasks if t.get('name') == self.task_config.get('name')), self.task_config)
        self.task_config = runtime_config
        self.task_data_sources = self.task_config.get('data_sources', [])
        # Re-initialize processor with updated config
        self.task_processor = TaskProcessorFactory.get_task_processor(
            processor_type=self.task_config.get('type', 'missing_data'),
            intermediate_storage=self.intermediate_storage,
            task_config = self.task_config
        )

        data = {}
        for data_key in self.task_data_sources:
            data[data_key] = self.intermediate_storage.load(data_key)
        execute_kwargs = {}
        if self.task_config.get('type') == 'custom':
            params = self.task_config.get('params', {})
            if isinstance(params, dict):
                execute_kwargs.update(params)
        result = self.task_processor.execute(data=data, **execute_kwargs)
        logger.info(f"Task '{self.task_config.get('name')}' executed with result: {result}")
        self.intermediate_storage.save(self.task_config.get('name'), result)
