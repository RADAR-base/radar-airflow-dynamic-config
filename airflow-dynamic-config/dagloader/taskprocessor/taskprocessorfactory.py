import dagloader.taskprocessor.missingdatataskprocessor as MissingDataTaskProcessor


class TaskProcessorFactory:
    @staticmethod
    def get_task_processor(processor_type: str) -> MissingDataTaskProcessor:
        if processor_type == 'data_checks':
            return MissingDataTaskProcessor()
        else:
            raise ValueError(f"Unsupported processor type: {processor_type}")