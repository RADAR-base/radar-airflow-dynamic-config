from airflow.models.param import Param
import logging

logger = logging.getLogger(__name__)


class ParamMaker():
    def __init__(self, config):
        self.config = config

    def generate_params(self):
        params = {}
        params = params | self._generate_sensor_param()
        params = params | self._generate_data_param()
        logger.info(f"Generated data params: {params}")
        params = params | self._generate_storage_param()
        params = params | self._generate_tasks_param()
        params = params | self._generate_action_param()
        return params

    def _generate_sensor_param(self):
        params = {}
        for sensor in self.config.get('sensor', []) or []:
            params[sensor['name']] = Param(sensor.get('config', {}) or {},
                                           type=["object", "null"])
        return params

    def _generate_data_param(self):
        params = {}
        for source in self.config.get('data', []) or []:
            params[source['name']] = Param(source.get('config', {}),
                                           type=["object", "null"])
        return params

    def _generate_storage_param(self):
        params = {}
        storage_config = self.config.get('intermediate_results_storage', {})
        params[storage_config['type']] = Param(storage_config['config'],
                                               type=["object", "null"])
        return params

    def _generate_tasks_param(self):
        params = {}
        tasks_config = self.config.get('tasks', [])
        for task in tasks_config:
            if 'config' not in task:
                task['config'] = {}
            params[task['name']] = Param(task['config'],
                                         type=["object", "null"])
        return params

    def _generate_action_param(self):
        params = {}
        for action_config in self.config.get('actions', []) or []:
            name = action_config['name']
            params[name] = Param(action_config.get('config', {}) or {},
                                 type=["object", "null"])
            condition = action_config.get('condition')
            if condition is not None:
                params[f"{name}_condition"] = Param(
                    condition, type=["string", "null"])
        return params