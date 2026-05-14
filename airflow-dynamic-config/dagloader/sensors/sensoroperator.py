from airflow.sensors.base import BaseSensorOperator
import logging

logger = logging.getLogger(__name__)


class ConfigurableSensorOperator(BaseSensorOperator):
    """Base sensor that lets DAG-level params override its YAML config.

    ParamMaker emits one Airflow Param per sensor keyed by its name (which
    DAGMaker also assigns as `task_id`). Before the first poke we merge any
    runtime override on top of the initial config and let the subclass
    re-derive whatever attributes it cached from it via `_resolve_config()`.
    """

    def __init__(self, sensor_config: dict, intermediate_storage=None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._initial_sensor_config = dict(sensor_config or {})
        self.sensor_name = self._initial_sensor_config.get('name', "default_sensor")
        self.sensor_config = dict(self._initial_sensor_config)
        self.intermediate_storage = intermediate_storage
        self._resolve_config()

    def _resolve_config(self):
        """Hook: subclasses derive cached attrs from `self.sensor_config` here.

        Invoked once from `__init__` and again from `pre_execute` whenever a
        runtime param override is applied.
        """

    def get_config(self, key: str, default=None):
        return self.sensor_config.get(key, default)

    def save_to_storage(self, value):
        key = self.sensor_name
        if self.intermediate_storage is None:
            return
        self.intermediate_storage.save(key, value)

    def pre_execute(self, context):
        super().pre_execute(context)
        params = (context or {}).get('params') or {}
        override = params.get(self.task_id)
        if not isinstance(override, dict) or not override:
            return
        merged = {**self._initial_sensor_config, **override}
        if merged == self.sensor_config:
            return
        logger.info(
            f"Sensor '{self.task_id}' applying runtime param override: "
            f"{sorted(override.keys())}"
        )
        self.sensor_config = merged
        self._resolve_config()
