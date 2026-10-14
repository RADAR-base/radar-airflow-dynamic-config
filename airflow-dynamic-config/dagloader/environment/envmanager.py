"""Per-pipeline Python environments for custom tasks.

A pipeline config may declare an environment::

    environment:
      name: mcraving_pipeline_env
      requirements:
        path: custom/mcraving_pipeline/requirements.txt   # relative to dags folder
      packages: [xgboost==2.1.1]                          # optional extra pins
      python: "3.11"                                      # optional, needs uv

The first task that needs it builds a virtualenv under ``PIPELINE_ENVS_DIR``
(a persistent volume, default ``/opt/airflow/envs``). Every later run on any
worker that mounts the same volume inherits the existing env instead of
rebuilding it.

Each env lives at ``<PIPELINE_ENVS_DIR>/<name>/<fingerprint>``, where the
fingerprint hashes the requirements and Python version. Changing the
requirements therefore yields a fresh env, while tasks still running on the old
one are not disturbed. A ``.ready`` marker is written only after a successful
install, so a half-built env is never reused.
"""
import contextlib
import hashlib
import json
import logging
import os
import pickle
import re
import shutil
import subprocess
import sys
import tempfile
import threading
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

DAGS_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
RUNNER_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'runner.py')
DEFAULT_ENVS_DIR = '/opt/airflow/envs'
READY_MARKER = '.ready'
_NAME_RE = re.compile(r'^[A-Za-z0-9][A-Za-z0-9_.-]*$')


def envs_base_dir() -> str:
    return os.environ.get('PIPELINE_ENVS_DIR', DEFAULT_ENVS_DIR)


@contextlib.contextmanager
def _file_lock(path: str):
    """Exclusive inter-process lock so two workers never build the same env."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'a+') as handle:
        try:
            import fcntl
        except ImportError:  # non-POSIX; best effort
            yield
            return
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


class PipelineEnvironmentError(RuntimeError):
    """Raised when an environment cannot be built or a task in it fails."""


class PipelineEnvironment:
    def __init__(self, name: str, requirements_path: Optional[str] = None,
                 packages: Optional[List[str]] = None,
                 python: Optional[str] = None,
                 base_dir: Optional[str] = None):
        if not name or not _NAME_RE.match(str(name)):
            raise ValueError(
                f"Invalid environment name {name!r}: use letters, digits, '_', '-', '.'."
            )
        self.name = str(name)
        self.requirements_path = (
            self._resolve(requirements_path) if requirements_path else None
        )
        self.packages = [str(p) for p in (packages or [])]
        self.python = str(python) if python else None
        self._base_dir = base_dir

    # ------------------------------------------------------------------ config
    @staticmethod
    def _resolve(path: str) -> str:
        return path if os.path.isabs(path) else os.path.join(DAGS_ROOT, path)

    @staticmethod
    def config_from_pipeline(config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Top-level env block. ``enviroment`` is accepted as a legacy spelling."""
        return config.get('environment') or config.get('enviroment')

    @classmethod
    def from_config(cls, env_config: Any) -> Optional['PipelineEnvironment']:
        """Build from a config block, or return None to use the Airflow env.

        ``None``, ``False`` or ``"default"`` mean "run in the Airflow env".
        """
        if env_config in (None, False) or env_config == 'default':
            return None
        if not isinstance(env_config, dict):
            raise ValueError(f"'environment' must be a mapping, got {env_config!r}")
        requirements = env_config.get('requirements')
        if isinstance(requirements, dict):
            requirements = requirements.get('path')
        return cls(
            name=env_config.get('name'),
            requirements_path=requirements,
            packages=env_config.get('packages'),
            python=env_config.get('python'),
        )

    # ------------------------------------------------------------------ layout
    @property
    def base_dir(self) -> str:
        return self._base_dir or envs_base_dir()

    def fingerprint(self) -> str:
        requirements = ''
        if self.requirements_path:
            if not os.path.isfile(self.requirements_path):
                raise FileNotFoundError(
                    f"Requirements file for environment '{self.name}' not found: "
                    f"{self.requirements_path}"
                )
            with open(self.requirements_path) as f:
                lines = [ln.strip() for ln in f.read().splitlines()]
            requirements = '\n'.join(ln for ln in lines if ln and not ln.startswith('#'))
        spec = {
            'requirements': requirements,
            'packages': sorted(self.packages),
            'python': self.python or f"{sys.version_info.major}.{sys.version_info.minor}",
        }
        blob = json.dumps(spec, sort_keys=True).encode()
        return hashlib.sha256(blob).hexdigest()[:16]

    def env_dir(self) -> str:
        return os.path.join(self.base_dir, self.name, self.fingerprint())

    def python_executable(self) -> str:
        if os.name == 'nt':
            return os.path.join(self.env_dir(), 'Scripts', 'python.exe')
        return os.path.join(self.env_dir(), 'bin', 'python')

    def is_ready(self) -> bool:
        return (os.path.isfile(os.path.join(self.env_dir(), READY_MARKER))
                and os.path.exists(self.python_executable()))

    # ------------------------------------------------------------------- build
    def ensure(self) -> str:
        """Return the env's interpreter, building the env on first use."""
        if self.is_ready():
            logger.info(f"Reusing environment '{self.name}' at {self.env_dir()}")
            return self.python_executable()

        lock_path = os.path.join(self.base_dir, self.name, '.lock')
        with _file_lock(lock_path):
            # Another worker may have finished the build while we waited.
            if self.is_ready():
                logger.info(f"Environment '{self.name}' was built by another worker")
                return self.python_executable()
            self._build()
        return self.python_executable()

    def _run(self, cmd: List[str]) -> None:
        logger.info(f"[env:{self.name}] $ {' '.join(cmd)}")
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                              text=True)
        for line in proc.stdout.splitlines():
            logger.info(f"[env:{self.name}] {line}")
        if proc.returncode != 0:
            raise PipelineEnvironmentError(
                f"Command failed ({proc.returncode}) while building environment "
                f"'{self.name}': {' '.join(cmd)}"
            )

    def _build(self) -> None:
        env_dir = self.env_dir()
        if os.path.exists(env_dir):  # leftover from a failed build
            shutil.rmtree(env_dir)
        os.makedirs(os.path.dirname(env_dir), exist_ok=True)
        logger.info(f"Creating environment '{self.name}' at {env_dir}")

        uv = shutil.which('uv')
        python = self.python_executable()
        try:
            if uv:
                self._run([uv, 'venv', '--python', self.python or sys.executable, env_dir])
                install = [uv, 'pip', 'install', '--python', python]
            else:
                if self.python:
                    raise PipelineEnvironmentError(
                        f"Environment '{self.name}' sets python={self.python!r}, "
                        "which needs 'uv' on the worker."
                    )
                self._run([sys.executable, '-m', 'venv', env_dir])
                install = [python, '-m', 'pip', 'install', '--disable-pip-version-check']

            if self.requirements_path:
                self._run(install + ['-r', self.requirements_path])
            if self.packages:
                self._run(install + self.packages)
        except BaseException:
            shutil.rmtree(env_dir, ignore_errors=True)
            raise

        with open(os.path.join(env_dir, READY_MARKER), 'w') as f:
            json.dump({'name': self.name,
                       'requirements': self.requirements_path,
                       'packages': self.packages}, f)
        logger.info(f"Environment '{self.name}' ready at {env_dir}")

    # --------------------------------------------------------------------- run
    def run_task_raw(self, path: str, inputs: Dict[str, bytes],
                     kwargs: Dict[str, Any],
                     func_name: Optional[str] = None) -> bytes:
        """Run a custom task inside this env on stored payloads.

        ``inputs`` maps each upstream name to its payload exactly as
        intermediate storage holds it. The return value is the encoded
        result, ready for ``Storage.save_raw``. The Airflow process never
        decodes either side, so the task's data only has to be readable by
        this env's packages.
        """
        python = self.ensure()
        with tempfile.TemporaryDirectory(prefix=f"task_{self.name}_") as work_dir:
            input_files = {}
            for i, (name, payload) in enumerate(inputs.items()):
                filename = f"input_{i}.pkl"
                with open(os.path.join(work_dir, filename), 'wb') as f:
                    f.write(payload)
                input_files[name] = filename
            with open(os.path.join(work_dir, 'meta.pkl'), 'wb') as f:
                pickle.dump({'path': path, 'func_name': func_name,
                             'kwargs': kwargs, 'inputs': input_files},
                            f, protocol=pickle.HIGHEST_PROTOCOL)

            child_env = dict(os.environ)
            child_env.pop('PYTHONPATH', None)
            child_env['VIRTUAL_ENV'] = self.env_dir()
            child_env['PATH'] = os.pathsep.join(
                [os.path.dirname(python), child_env.get('PATH', '')]
            )

            cmd = [python, '-I', RUNNER_PATH, work_dir]
            logger.info(f"Running '{path}' in environment '{self.name}'")
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, text=True,
                                    env=child_env, cwd=DAGS_ROOT)
            relay = threading.Thread(target=self._relay, args=(proc.stdout,),
                                     daemon=True)
            relay.start()
            returncode = proc.wait()
            relay.join()

            result_path = os.path.join(work_dir, 'result.pkl')
            error_path = os.path.join(work_dir, 'error.txt')
            if returncode == 0 and os.path.isfile(result_path):
                with open(result_path, 'rb') as f:
                    return f.read()
            error = ''
            if os.path.isfile(error_path):
                with open(error_path) as f:
                    error = f.read()
            raise PipelineEnvironmentError(
                f"Task '{path}' failed in environment '{self.name}' "
                f"(exit code {returncode}):\n{error}"
            )

    def _relay(self, stream) -> None:
        for line in stream:
            logger.info(f"[env:{self.name}] {line.rstrip()}")

    def __repr__(self) -> str:
        return f"PipelineEnvironment(name={self.name!r})"
