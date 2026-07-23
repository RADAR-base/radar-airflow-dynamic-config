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
                 intermediate_storage, trigger_assets=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.intermediate_storage = intermediate_storage
        self.data_config = data_config
        # Assets of the watchers this source depends on. When the DAG run was
        # started by one of them, the matched key payloads (e.g. participant_id)
        # are read from the run's triggering asset events and used to filter.
        self.trigger_assets = list(trigger_assets or [])
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
        self.source_config = new_source_config
        self._resolve_config()

    def _iter_event_payloads(self, context):
        """Yield the matched ``{field: value}`` payload of every triggering
        asset event emitted by the watcher(s) this source depends on."""
        triggering = (context or {}).get('triggering_asset_events') or {}
        wanted = {getattr(a, 'name', None) for a in self.trigger_assets}
        for asset in triggering:
            if getattr(asset, 'name', None) not in wanted:
                continue
            for event in triggering[asset]:
                payload = (getattr(event, 'extra', None) or {}).get('payload')
                if isinstance(payload, dict) and payload:
                    yield payload

    def _collect_key_filters(self, context):
        """De-duplicated list of matched key payloads from the run's triggering
        asset events (e.g. ``[{"participant_id": "p1"}, {"participant_id": "p2"}]``).

        Every event in the run is included so coalesced bursts are all
        processed. Empty when the run was not asset triggered (e.g. a manual
        run), which leaves the read unfiltered."""
        if not self.trigger_assets:
            return []
        filters = []
        seen = set()
        for payload in self._iter_event_payloads(context):
            dedup_key = tuple(sorted(payload.items()))
            if dedup_key not in seen:
                seen.add(dedup_key)
                filters.append(payload)
        return filters

    def execute(self, context):
        reader_kwargs = dict(self.reader_kwargs)
        # Give each DAG/source its own consumer group so offsets don't overlap
        # with other DAGs or external consumers sharing the connection's group.
        if self.source_type == 'kafka' and not reader_kwargs.get('group_id'):
            dag_id = getattr(self, 'dag_id', None) or 'radar'
            reader_kwargs['group_id'] = f"{dag_id}.{self.source_name}"
        key_filters = self._collect_key_filters(context)
        if key_filters:
            logger.info(
                f"Data source '{self.source_name}' filtering to matched keys: "
                f"{key_filters}"
            )
            reader_kwargs['key_filters'] = key_filters
        reader = DataReaderFactory.get_data_reader(
            self.source_type, **reader_kwargs
        )
        data = reader.read_data()
        self.intermediate_storage.save(self.source_name, data)
