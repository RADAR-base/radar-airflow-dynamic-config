import importlib.util
import inspect
import os

from dagloader.taskprocessor.taskprocessor import TaskProcessor

class CustomTaskProcessor(TaskProcessor):
    def __init__(self, intermediate_storage, path):
        self.intermediate_storage = intermediate_storage
        self.func_name = "execute"
        self.path = path
        super().__init__()

    def execute(self, data, **kwargs):
        if not self.path:
            raise ValueError("Custom task requires a 'path' value.")

        func_name = kwargs.get('func_name')
        if func_name:
            self.func_name = func_name

        if os.path.sep in self.path or self.path.endswith('.py') or '/' in self.path:
            module = self._load_module_from_path(self.path)
            processing_func = self._resolve_callable(module, self.func_name)
        else:
            module_path, func_name = self.path.rsplit('.', 1)
            module = __import__(module_path, fromlist=[func_name])
            processing_func = getattr(module, func_name)

        return self._run_processing_func(processing_func, data, kwargs)

    def _load_module_from_path(self, path):
        if not os.path.isabs(path):
            dags_root = os.path.abspath(
                os.path.join(os.path.dirname(__file__), '..', '..')
            )
            path = os.path.join(dags_root, path)

        module_name = f"custom_task_{abs(hash(path))}"
        spec = importlib.util.spec_from_file_location(module_name, path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Unable to load module from path: {path}")

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def _resolve_callable(self, module, func_name):
        if func_name:
            target = getattr(module, func_name, None)
            if target is not None:
                if inspect.isclass(target):
                    instance = target()
                    return instance.execute
                return target

        for _, cls in inspect.getmembers(module, inspect.isclass):
            if cls.__module__ == module.__name__ and hasattr(cls, 'execute'):
                instance = cls()
                return instance.execute

        if hasattr(module, 'execute'):
            return getattr(module, 'execute')

        raise AttributeError(
            f"Module '{module.__name__}' has no callable target to execute."
        )

    def _run_processing_func(self, processing_func, data, kwargs):
        if not callable(processing_func):
            raise ValueError("Resolved processing target is not callable.")

        safe_kwargs = dict(kwargs)
        safe_kwargs.pop('func_name', None)

        signature = inspect.signature(processing_func)
        params = signature.parameters

        if 'intermediate_storage' in params:
            safe_kwargs.setdefault('intermediate_storage', self.intermediate_storage)

        has_var_kwargs = any(
            param.kind == param.VAR_KEYWORD for param in params.values()
        )

        if has_var_kwargs:
            return processing_func(data, **safe_kwargs)

        filtered_kwargs = {key: value for key, value in safe_kwargs.items()
                           if key in params}
        return processing_func(data, **filtered_kwargs)