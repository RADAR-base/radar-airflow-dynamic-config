from datetime import datetime, timedelta
import logging
from dagloader.configloader import ConfigLoader
from airflow import DAG
from dagloader.datareader.datareaderoperator import DataReaderOperator
from dagloader.taskprocessor.taskoperator  import TaskOperator
from dagloader.intermediatestorage.storagefactory import StorageFactory
from dagloader.conditionparser import ConditionOperator
from dagloader.actionparser import ActionOperator
from typing import Dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DAGMaker:
    def __init__(self, config_path: str):
        self.config_loader = ConfigLoader(config_path)
        self.config = self.config_loader.get_config()
        intermediate_storage_config = self.config.get('intermediate_results_storage', {})
        self.storage = StorageFactory.get_storage(
            storage_type=intermediate_storage_config.get('type', 'local'),
            **intermediate_storage_config.get('config', {})
        )
        self.model_name = self.config.get('model_name', 'unknown').lower()
        self.storage.init(directory_name=self.model_name)

    def generate_task_dependencies(self, data_dags: Dict, task_dags: Dict, action_dags: Dict) -> Dict:
        """
        This function will generate task dependencies based on data DAGs and task DAGs.
        It will return a dictionary of DAGs with their tasks and dependencies.
        Algo:
        """
        dag_tasks = []
        dag_tasks = self.generate_data_task_dependencies(data_dags, task_dags)
        dag_tasks = self.generate_task_action_dependencies(dag_tasks, action_dags)
        return dag_tasks

    def parse_source_types(self, data_configs: Dict) -> Dict:
        """
        This function will parse the source types from the config.
        It will return a dictionary of source types with their configurations.
        """
        source_types = data_configs['source_types']
        data_reader_tasks = {}
        for source in source_types:
            data_reader_tasks[source['name']] = DataReaderOperator(
                task_id=f"{source['name']}",
                reader_config=source['config']
                )
        return data_reader_tasks

    def parse_data_dags(self) -> Dict:
        """
        This function will parse the data DAGs from the config.
        It will return a dictionary of data DAGs with their schedules and tasks.
        """
        data_configs = self.config.get('data', [])
        self.project_id = self.config.get('project')
        self.source_types = self.config.get('source_types', [])
        data_dags = self.parse_source_types(data_configs)
        logger.info(f"Parsed data DAGs: {data_dags}")
        return data_dags

    def parse_tasks(self) -> Dict:
        """
        This function will parse the tasks from the config.
        It will return a dictionary of tasks with their configurations.
        """
        task_configs = self.config.get('tasks', [])
        tasks_dags = {}
        for task in task_configs:
            tasks_dags[task['name']] = TaskOperator(
                task_id=f"{task['name']}",
                processor_type=task.get('type', 'missing_data')
            )
        logger.info(f"Parsed task DAGs: {tasks_dags}")
        return tasks_dags

    def parse_actions_and_conditions(self) -> Dict:
        """
        This function will parse the actions from the config.
        It will return a dictionary of actions with their configurations.
        """
        actions_configs = self.config.get('actions', [])
        action_dags = {}
        for task in task_configs:
            for action in task.get('actions', []):
                conditions_dags = []
                for i, conditions in enumerate(action.get('conditions', [])):
                    condition_parser = ConditionParser(
                        condition_str=conditions,
                        condition_name=f"{action.get('type',
                        'default_condition')}_{i}"
                    )
                    conditions_dags.append(condition_parser)
                if action['type'] == 'send_notification':
                    conditions = action.get('conditions', {})
        return action_dags


    def parse_configs(self) -> Dict:
        """
        This function will parse the configs and create a list of DAGs with their tasks.
        Each DAG will correspond to a unique schedule found in the config.
        """
        data_dags = self.parse_data_dags()
        task_dags = self.parse_tasks()
        action_tasks = self.parse_actions_and_conditions()
        logger.info(f"Action tasks: {action_tasks}")
        dag_tasks = self.generate_task_dependencies(data_dags, task_dags, action_tasks)
        return dag_tasks

    def generate_dags(self):
        # Logic to create DAG based on self.config
        dag_id = f"{self.config.get('model_name', 'unknown').lower()}"
        dag_name = f"{dag_id}_dag"
        dag_schedule = self.config.get('schedule', '@daily')
        default_args = {
            'owner': 'airflow',
            'depends_on_past': False,
            'email_on_failure': True,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        }
        with DAG(
            dag_id=dag_name,
            default_args=default_args,
            description=self.config.get('model_description', ''),
            schedule=dag_schedule,
            start_date=datetime(2024, 1, 1),
            catchup=False,
            tags=['radar', 'dynamic', 'python-class'],
        ) as dag:
            dag_tasks = self.parse_configs()
            logger.info(f"Creating DAG: {dag_id} with tasks: {dag_tasks}")
        return dag
