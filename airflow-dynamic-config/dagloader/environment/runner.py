"""Entry point for a custom task executed inside a pipeline virtualenv.

Invoked by ``PipelineEnvironment.run_task_raw`` as::

    <venv>/bin/python -I runner.py <work_dir>

``<work_dir>`` contains:

* ``meta.pkl``: ``{path, func_name, kwargs, inputs: {upstream_name: filename}}``
* one file per upstream holding that upstream's stored payload, exactly as
  intermediate storage keeps it (pickle bytes), never re-encoded by Airflow

The runner decodes the inputs, calls the task, and writes either
``result.pkl`` (the encoded result, saved to storage as-is by Airflow),
``skip.txt`` (the task raised ``AirflowSkipException``; holds its message) or
``error.txt`` (a traceback). Only the standard library and the venv's own
packages are available here.
"""
import logging
import os
import pickle
import sys
import traceback

# Make `dagloader` (and anything else in the dags folder) importable. `-I`
# drops the script dir and PYTHONPATH from sys.path, so add the root here.
DAGS_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if DAGS_ROOT not in sys.path:
    sys.path.insert(0, DAGS_ROOT)

from dagloader.taskprocessor import customloader  # noqa: E402

RESULT_FILE = 'result.pkl'
ERROR_FILE = 'error.txt'
SKIP_FILE = 'skip.txt'
# Matched by name so both Airflow's class and the stand-in from
# dagloader.taskprocessor.exceptions count as a skip.
SKIP_EXCEPTION_NAME = 'AirflowSkipException'


def main(work_dir):
    logging.basicConfig(
        level=logging.INFO, stream=sys.stderr,
        format='%(levelname)s %(name)s: %(message)s',
    )
    try:
        with open(os.path.join(work_dir, 'meta.pkl'), 'rb') as f:
            meta = pickle.load(f)
        data = {}
        for name, filename in meta['inputs'].items():
            with open(os.path.join(work_dir, filename), 'rb') as f:
                data[name] = pickle.load(f)
        func = customloader.resolve_target(
            meta['path'], meta.get('func_name') or 'execute'
        )
        result = customloader.run_processing_func(
            func, data, meta.get('kwargs') or {}, storage_available=False,
        )
        tmp_path = os.path.join(work_dir, RESULT_FILE + '.tmp')
        with open(tmp_path, 'wb') as f:
            pickle.dump(result, f, protocol=pickle.HIGHEST_PROTOCOL)
        os.replace(tmp_path, os.path.join(work_dir, RESULT_FILE))
        return 0
    except BaseException as exc:
        if type(exc).__name__ == SKIP_EXCEPTION_NAME:
            with open(os.path.join(work_dir, SKIP_FILE), 'w') as f:
                f.write(str(exc))
            return 0
        with open(os.path.join(work_dir, ERROR_FILE), 'w') as f:
            f.write(traceback.format_exc())
        return 1


if __name__ == '__main__':
    sys.exit(main(sys.argv[1]))
