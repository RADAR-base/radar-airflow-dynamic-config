from airflow.sdk import BaseOperator
from dagloader.taskprocessor.taskprocessorfactory import TaskProcessorFactory
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
    """

    def __init__(self, task_config, intermediate_storage, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.intermediate_storage = intermediate_storage
        self._initial_task_config = copy.deepcopy(task_config) or {}
        self.task_config = copy.deepcopy(self._initial_task_config)
        self._resolve_config()

    def _resolve_config(self):
        self.processor = TaskProcessorFactory.get_task_processor(
            processor_type=self.task_config.get('type', 'missing_data'),
            intermediate_storage=self.intermediate_storage,
            task_config=self.task_config,
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
        data = {}
        for upstream in self.task_config.get('depends_on', []) or []:
            data[upstream] = self.intermediate_storage.load(upstream)

        execute_kwargs = {}
        if self.task_config.get('type') == 'custom':
            kwargs_source = (
                self.task_config.get('config')
                or self.task_config.get('params')
                or {}
            )
            if isinstance(kwargs_source, dict):
                execute_kwargs.update(kwargs_source)

        result = self.processor.execute(data=data, **execute_kwargs)
        logger.info(
            f"Task '{self.task_config.get('name')}' executed with result: {result}"
        )
        self.intermediate_storage.save(self.task_config.get('name'), result)
