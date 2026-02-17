from airflow.sdk import BaseOperator
from dagloader.taskprocessor.taskprocessorfactory import TaskProcessorFactory


class TaskOperator(BaseOperator):
    def __init__(self, task_config, intermediate_storage, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.intermediate_storage = intermediate_storage
        self.task_config = task_config
        self.task_processor = TaskProcessorFactory.get_task_processor(
            processor_type=self.task_config.get('type', 'missing_data'),
            intermediate_storage=self.intermediate_storage
        )
        self.task_data_sources = self.task_config.get('data_sources', [])

    def execute(self, context):
        data = {}
        for data_key in self.task_data_sources:
            data[data_key] = self.intermediate_storage.load(data_key)
        result = self.task_processor.execute(data=data)
        self.intermediate_storage.save(self.task_config.get('name'), result)
