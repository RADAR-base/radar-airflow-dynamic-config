from dagloader.taskprocessor.taskprocessor import TaskProcessor

class CustomTaskProcessor(TaskProcessor):
    def __init__(self, intermediate_storage):
        self.intermediate_storage = intermediate_storage
        super().__init__()

    def execute(self, data, path, **kwargs):
        # Dynamically import the custom processing function from the specified path
        module_path, func_name = path.rsplit('.', 1)
        module = __import__(module_path, fromlist=[func_name])
        processing_func = getattr(module, func_name)

        # Execute the custom processing function with the provided data
        result = processing_func(data, self.intermediate_storage, **kwargs)
        return result