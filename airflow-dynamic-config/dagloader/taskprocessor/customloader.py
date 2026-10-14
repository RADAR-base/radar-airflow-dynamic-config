"""Resolve and call a custom task's entry point.

Standard library only: this module is imported both by the Airflow worker
(in-process custom tasks) and by ``dagloader/environment/runner.py`` inside a
pipeline's own virtualenv, which has no Airflow installed.
"""
import importlib
import importlib.util
import inspect
import os
import sys

# Root of the dags folder; relative custom task paths resolve against it.
DAGS_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))


def resolve_path(path):
    """Absolute path for a file referenced relative to the dags folder."""
    if os.path.isabs(path):
        return path
    return os.path.join(DAGS_ROOT, path)


def is_file_path(path):
    return os.path.sep in path or '/' in path or path.endswith('.py')


def load_module_from_path(path):
    path = resolve_path(path)
    module_name = f"custom_task_{abs(hash(path))}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load module from path: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def resolve_callable(module, func_name):
    if func_name:
        target = getattr(module, func_name, None)
        if target is not None:
            if inspect.isclass(target):
                return target().execute
            return target

    for _, cls in inspect.getmembers(module, inspect.isclass):
        if cls.__module__ == module.__name__ and hasattr(cls, 'execute'):
            return cls().execute

    if hasattr(module, 'execute'):
        return getattr(module, 'execute')

    raise AttributeError(
        f"Module '{module.__name__}' has no callable target to execute."
    )


def resolve_target(path, func_name='execute'):
    """Return the callable a custom task's ``path`` points at.

    ``path`` is either a ``.py`` file (relative to the dags folder or absolute)
    or a dotted ``package.module.callable`` import path.
    """
    if not path:
        raise ValueError("Custom task requires a 'path' value.")
    if is_file_path(path):
        module = load_module_from_path(path)
        return resolve_callable(module, func_name)
    module_path, attr = path.rsplit('.', 1)
    module = importlib.import_module(module_path)
    return getattr(module, attr)


def run_processing_func(processing_func, data, kwargs, intermediate_storage=None,
                        storage_available=True):
    """Call ``processing_func(data, **kwargs)`` passing only accepted kwargs.

    ``intermediate_storage`` is injected when the callable asks for it. In an
    isolated environment the storage object cannot cross the process boundary,
    so ``storage_available`` is False and the argument is left out.
    """
    if not callable(processing_func):
        raise ValueError("Resolved processing target is not callable.")

    safe_kwargs = dict(kwargs)
    safe_kwargs.pop('func_name', None)

    params = inspect.signature(processing_func).parameters

    if 'intermediate_storage' in params:
        if storage_available:
            safe_kwargs.setdefault('intermediate_storage', intermediate_storage)
        else:
            print(
                "WARNING: task requests 'intermediate_storage', which is not "
                "available when running in a separate environment; it is not "
                "passed.",
                file=sys.stderr,
            )

    has_var_kwargs = any(
        param.kind == param.VAR_KEYWORD for param in params.values()
    )
    if has_var_kwargs:
        return processing_func(data, **safe_kwargs)

    filtered_kwargs = {key: value for key, value in safe_kwargs.items()
                       if key in params}
    return processing_func(data, **filtered_kwargs)
