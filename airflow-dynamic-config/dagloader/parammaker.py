from airflow.sdk import Param
from dagloader.environment import PipelineEnvironment
import logging

logger = logging.getLogger(__name__)

# DAG param holding the pipeline-level `environment` block.
ENVIRONMENT_PARAM = 'environment'


class ParamMaker():
    def __init__(self, config):
        self.config = config

    def generate_params(self):
        params = {}
        params = params | self._generate_sensor_param()
        params = params | self._generate_data_param()
        params = params | self._generate_storage_param()
        params = params | self._generate_tasks_param()
        params = params | self._generate_action_param()
        env_param = self._generate_environment_param()
        if env_param and ENVIRONMENT_PARAM in params:
            raise ValueError(
                f"'{ENVIRONMENT_PARAM}' is reserved for the pipeline "
                "environment param; rename the step that uses it."
            )
        params = params | env_param
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

    def _generate_environment_param(self):
        # Only pipelines that declare an env get the param. A mapping
        # selects/edits the env; null or "default" runs in the Airflow env.
        env_config = PipelineEnvironment.config_from_pipeline(self.config)
        if env_config is None:
            return {}
        return {ENVIRONMENT_PARAM: Param(env_config,
                                         type=["object", "string", "null"])}
