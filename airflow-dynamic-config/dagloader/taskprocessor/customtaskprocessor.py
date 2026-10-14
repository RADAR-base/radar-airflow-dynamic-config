import logging
import pickle

from dagloader.taskprocessor import customloader
from dagloader.taskprocessor.taskprocessor import TaskProcessor

logger = logging.getLogger(__name__)


class CustomTaskProcessor(TaskProcessor):
    """Run a user-supplied callable.

    Without an ``environment`` the callable runs in the Airflow worker process.
    With one, it runs in a subprocess using that pipeline's own virtualenv,
    which is created on first use and reused afterwards.
    """

    def __init__(self, intermediate_storage, path, environment=None):
        self.intermediate_storage = intermediate_storage
        self.func_name = "execute"
        self.path = path
        self.environment = environment
        super().__init__()

    def execute(self, data, **kwargs):
        if not self.path:
            raise ValueError("Custom task requires a 'path' value.")

        func_name = kwargs.get('func_name')
        if func_name:
            self.func_name = func_name

        if self.environment is not None:
            # Callers holding decoded data; the operator uses execute_raw.
            inputs = {name: pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
                      for name, value in data.items()}
            return pickle.loads(self.execute_raw(inputs, **kwargs))

        processing_func = customloader.resolve_target(self.path, self.func_name)
        return customloader.run_processing_func(
            processing_func, data, kwargs,
            intermediate_storage=self.intermediate_storage,
        )

    def execute_raw(self, inputs, **kwargs):
        """Run in the task's separate env on stored payloads.

        ``inputs`` maps upstream names to payload bytes as kept by
        intermediate storage; returns the encoded result for ``save_raw``.
        """
        if self.environment is None:
            raise RuntimeError("execute_raw requires a separate environment.")
        if not self.path:
            raise ValueError("Custom task requires a 'path' value.")
        func_name = kwargs.get('func_name') or self.func_name
        task_kwargs = {k: v for k, v in kwargs.items() if k != 'func_name'}
        return self.environment.run_task_raw(
            path=self.path, inputs=inputs, kwargs=task_kwargs,
            func_name=func_name,
        )
