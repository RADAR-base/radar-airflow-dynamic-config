from airflow.sdk import BaseOperator
from dagloader.taskprocessor.taskprocessorfactory import TaskProcessorFactory


class TaskOperator(BaseOperator):
    def __init__(self, processor_type: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.task_processor = TaskProcessorFactory.get_task_processor(
            processor_type)

    def execute(self, context):
        self.task_processor.get_processor_task()

    def get_data_processor_task(self, **kwargs):
        return self.task_processor.get_data_processor_task(**kwargs)