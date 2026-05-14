from airflow.sensors.base import BaseSensorOperator


class ConfigurableSensorOperator(BaseSensorOperator):
    def __init__(self, sensor_config: dict, intermediate_storage=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sensor_config = sensor_config or {}
        self.intermediate_storage = intermediate_storage  #sym:intermediate_storage

    def get_config(self, key: str, default=None):
        return self.sensor_config.get(key, default)

    def save_to_storage(self, key: str, value):
        if self.intermediate_storage is None:
            return
        self.intermediate_storage.save(key, value)