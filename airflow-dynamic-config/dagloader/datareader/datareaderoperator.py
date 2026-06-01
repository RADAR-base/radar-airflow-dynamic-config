from airflow.sdk import BaseOperator
from dagloader.datareader.datareaderfactory import DataReaderFactory
import copy
import logging

logger = logging.getLogger(__name__)


class DataReaderOperator(BaseOperator):
    """Data-source operator driven by a unified-format data entry.

    Mirrors the sensor / task pattern:
      - `_initial_source_config` holds the pristine YAML entry.
      - `source_config` is the active version mutated by `pre_execute`.
      - `_resolve_config()` caches `source_name`, `source_type`,
        `reader_kwargs` from `source_config`.
    ParamMaker emits one Param per data source keyed by name (= `task_id`)
    whose default is the source's `config` block; the override merges
    into `source_config['config']`.
    """

    def __init__(self, data_config, source_config: dict,
                 intermediate_storage, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.intermediate_storage = intermediate_storage
        self.data_config = data_config
        self._initial_source_config = copy.deepcopy(source_config) or {}
        self.source_config = copy.deepcopy(self._initial_source_config)
        self._resolve_config()

    def _resolve_config(self):
        self.source_name = self.source_config.get('name')
        self.source_type = self.source_config.get('type')
        self.reader_kwargs = self.source_config.get('config', {}) or {}

    def pre_execute(self, context):
        super().pre_execute(context)
        params = (context or {}).get('params') or {}
        override = params.get(self.task_id)
        if not isinstance(override, dict) or not override:
            return
        new_source_config = copy.deepcopy(self._initial_source_config)
        base_config = new_source_config.get('config') or {}
        new_source_config['config'] = {**base_config, **override}
        if new_source_config == self.source_config:
            return
        logger.info(
            f"Data source '{self.task_id}' applying runtime config override: "
            f"{sorted(override.keys())}"
        )
        self.source_config = new_source_config
        self._resolve_config()

    def execute(self, context):
        reader_kwargs = dict(self.reader_kwargs)
        # Give each DAG/source its own consumer group so offsets don't overlap
        # with other DAGs or external consumers sharing the connection's group.
        if self.source_type == 'kafka' and not reader_kwargs.get('group_id'):
            dag_id = getattr(self, 'dag_id', None) or 'radar'
            reader_kwargs['group_id'] = f"{dag_id}.{self.source_name}"
        reader = DataReaderFactory.get_data_reader(
            self.source_type, **reader_kwargs
        )
        data = reader.read_data()
        self.intermediate_storage.save(self.source_name, data)
