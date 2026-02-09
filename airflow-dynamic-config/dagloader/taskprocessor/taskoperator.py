from airflow.sdk import BaseOperator
from dagloader.taskprocessor.taskprocessorfactory import TaskProcessorFactory


class TaskOperator(BaseOperator):
    def __init__(self, processor_type: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.intermediate_storage = kwargs.get('intermediate_storage')
        self.data_key = kwargs.get('reader_name')
        self.task_processor = TaskProcessorFactory.get_task_processor(
            processor_type,)

    def execute(self, context):
        data = self.intermediate_storage.load(self.data_key)
        self.task_processor.get_processor_task(data=data)