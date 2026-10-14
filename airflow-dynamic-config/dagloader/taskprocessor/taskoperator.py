from airflow.sdk import BaseOperator
from dagloader.taskprocessor.taskprocessorfactory import TaskProcessorFactory
from dagloader.environment import PipelineEnvironment
import copy
import logging

logger = logging.getLogger(__name__)


class TaskOperator(BaseOperator):
    """Task operator driven by a unified-format task entry.

    Same pattern as the sensors:
      - `_initial_task_config` holds the pristine YAML entry.
      - `task_config` is the active version mutated by `pre_execute`.
      - `_resolve_config()` derives cached attrs (here, the processor)
        from `task_config`; called from `__init__` and again from
        `pre_execute` after a runtime Param override is merged.
    ParamMaker emits one Param per task keyed by name (= `task_id`)
    whose default is the task's `config` block; that override merges
    into `task_config['config']`.

    Custom tasks run in a separate Python environment when one is set. The
    task's own `environment` key wins over the pipeline-level block passed as
    `environment_config`; `environment: default` forces the Airflow env. The
    env is only built at execute time on the worker, never at DAG parse time.
    """

    def __init__(self, task_config, intermediate_storage, *args,
                 environment_config=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.intermediate_storage = intermediate_storage
        self._pipeline_environment_config = copy.deepcopy(environment_config)
        self._initial_task_config = copy.deepcopy(task_config) or {}
        self.task_config = copy.deepcopy(self._initial_task_config)
        self._resolve_config()

    def _resolve_environment(self):
        if self.task_config.get('type') != 'custom':
            return None
        if 'environment' in self.task_config:
            env_config = self.task_config.get('environment')
        else:
            env_config = self._pipeline_environment_config
        return PipelineEnvironment.from_config(env_config)

    def _resolve_config(self):
        self.environment = self._resolve_environment()
        self.processor = TaskProcessorFactory.get_task_processor(
            processor_type=self.task_config.get('type', 'missing_data'),
            intermediate_storage=self.intermediate_storage,
            task_config=self.task_config,
            environment=self.environment,
        )

    def pre_execute(self, context):
        super().pre_execute(context)
        params = (context or {}).get('params') or {}
        override = params.get(self.task_id)
        if not isinstance(override, dict) or not override:
            return
        new_task_config = copy.deepcopy(self._initial_task_config)
        base_config = new_task_config.get('config') or {}
        new_task_config['config'] = {**base_config, **override}
        if new_task_config == self.task_config:
            return
        logger.info(
            f"Task '{self.task_id}' applying runtime config override: "
            f"{sorted(override.keys())}"
        )
        self.task_config = new_task_config
        self._resolve_config()

    def execute(self, context):
        upstreams = self.task_config.get('depends_on', []) or []
        name = self.task_config.get('name')

        execute_kwargs = {}
        if self.task_config.get('type') == 'custom':
            kwargs_source = (
                self.task_config.get('config')
                or self.task_config.get('params')
                or {}
            )
            if isinstance(kwargs_source, dict):
                execute_kwargs.update(kwargs_source)

        if self.environment is not None:
            # Separate env: hand the stored bytes through untouched and save
            # the bytes that come back, without decoding in this process.
            inputs = {
                upstream: self.intermediate_storage.load_raw(upstream)
                for upstream in upstreams
            }
            payload = self.processor.execute_raw(inputs, **execute_kwargs)
            logger.info(
                f"Task '{name}' executed in environment "
                f"'{self.environment.name}'; result is {len(payload)} bytes"
            )
            self.intermediate_storage.save_raw(name, payload)
            return

        data = {
            upstream: self.intermediate_storage.load(upstream)
            for upstream in upstreams
        }
        result = self.processor.execute(data=data, **execute_kwargs)
        logger.info(f"Task '{name}' executed with result: {result}")
        self.intermediate_storage.save(name, result)
