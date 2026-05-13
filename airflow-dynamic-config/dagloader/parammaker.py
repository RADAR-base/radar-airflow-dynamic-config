from airflow.models.param import Param
import logging

logger = logging.getLogger(__name__)

class ParamMaker():
    def __init__(self, config):
        self.config = config

    def generate_params(self):
        params = {}
        params = params | self._generate_data_param()
        logger.info(f"Generated data params: {params}")
        params = params | self._generate_storage_param()
        params = params | self._generate_tasks_param()
        params = params | self._generate_action_param()
        return params

    def _generate_data_param(self):
        params = {}
        data_config = self.config.get('data', {})
        for source_type in data_config['source_types']:
            params[source_type['name']] = Param(source_type['config'],
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
        action_configs = self.config.get('actions', {})
        for action_config in action_configs:
            if 'config' not in action_config:
                action_config['config'] = {}
            params[action_config['name']] = Param(action_config['config'],
                                                 type=["object", "null"])
            params[f"{action_config['name']}_condition"] = Param(
                action_config['condition'], type=["string", "null"])
        return params