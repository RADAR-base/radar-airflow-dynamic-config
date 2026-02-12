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

    def generate_data_task_dependencies(self, data_dags: Dict, task_dags: Dict) -> Dict:
        """
        This function will generate task dependencies for data tasks and processing tasks.
        It will return a dictionary of DAGs with their tasks and dependencies.
        """
        dag_tasks = {}

        # Add all data reader tasks to the dictionary
        for data_name, data_task in data_dags.items():
            dag_tasks[data_name] = data_task

        # Add all processing tasks and set their dependencies on data tasks
        for task_name, task in task_dags.items():
            dag_tasks[task_name] = task

            # Get data sources this task depends on
            task_data_sources = task.task_data_sources

            # Set dependencies: task depends on its data sources
            for data_source in task_data_sources:
                if data_source in dag_tasks:
                    dag_tasks[data_source] >> task
                else:
                    logger.warning(f"Data source '{data_source}' not found for task '{task_name}'")
        return dag_tasks

    def generate_task_action_dependencies(self, dag_tasks: Dict, action_dags: Dict) -> Dict:
        """
        This function will generate dependencies between tasks and actions.
        It will return the updated dictionary of DAGs with action tasks and dependencies.
        Note: This handles direct dependencies without conditions.
        """
        # Add all action tasks and set their dependencies on processing tasks
        for action_name, action_task in action_dags.items():
            dag_tasks[action_name] = action_task
            # Get the keys (tasks) this action depends on
            action_keys = action_task.keys
            # Set dependencies: action depends on its required tasks
            for key in action_keys:
                if key in dag_tasks:
                    dag_tasks[key] >> action_task
                else:
                    logger.warning(f"Dependency '{key}' not found for action '{action_name}'")

        return dag_tasks

    def generate_condition_action_dependencies(self, dag_tasks: Dict, condition_dags: Dict, action_dags: Dict) -> Dict:
        """
        This function will generate conditional dependencies between tasks, conditions, and actions.
        It sets up branch operators that evaluate conditions and route to actions accordingly.
        The flow is: task -> condition (branch) -> action (if condition is true)
        """
        # Add condition branch operators and link them between tasks and actions
        for action_name, condition_task in condition_dags.items():
            # Add condition task to dag_tasks with a unique key
            condition_key = f"{action_name}_condition"
            dag_tasks[condition_key] = condition_task
            # Get the dependencies from the action config
            action_keys = condition_task.action_config.get('depends_on', [])
            # Set dependencies: condition depends on its required tasks
            for key in action_keys:
                if key in dag_tasks:
                    dag_tasks[key] >> condition_task
                else:
                    logger.warning(f"Dependency '{key}' not found for condition '{action_name}'")
            # Link condition to action - condition branches to action if true
            if action_name in action_dags:
                dag_tasks[action_name] = action_dags[action_name]
                condition_task >> dag_tasks[action_name]
            else:
                logger.warning(f"Action '{action_name}' not found for condition '{action_name}'")
        return dag_tasks

    def generate_task_dependencies(self, data_dags: Dict, task_dags: Dict,
                                   condition_dags: Dict, action_dags: Dict) -> Dict:
        """
        This function will generate task dependencies based on data DAGs and task DAGs.
        It will return a dictionary of DAGs with their tasks and dependencies.
        Algo:
        1. Generate data -> task dependencies
        2. If conditions exist, generate task -> condition -> action dependencies
        3. Otherwise, generate task -> action dependencies directly
        """
        dag_tasks = {}
        dag_tasks = self.generate_data_task_dependencies(data_dags, task_dags)
        logger.info(f"Generated data -> task dependencies: {dag_tasks}")
        # If conditions are defined, use conditional branching
        if condition_dags:
            dag_tasks = self.generate_condition_action_dependencies(dag_tasks, condition_dags, action_dags)
        else:
            # Otherwise, create direct task -> action dependencies
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
                data_config=data_configs,
                source_config=source,
                intermediate_storage=self.storage
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
                task_config=task,
                intermediate_storage=self.storage
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
        condition_dags = {}
        for action in actions_configs:
            action_name = action['name']
            action_conditions = action.get('condition', '')
            data_keys = action.get('depends_on', [])
            condition_operator = ConditionOperator(
                action_config=action,
                intermediate_storage=self.storage,
                task_id=f"{action_name}_branch"
            )
            condition_dags[action_name] = condition_operator
            action_dags[action_name] = ActionOperator(
                task_id=f"{action_name}",
                action_config=action,
                intermediate_storage=self.storage
            )
        return action_dags, condition_dags

    def parse_configs(self) -> Dict:
        """
        This function will parse the configs and create a list of DAGs with their tasks.
        Each DAG will correspond to a unique schedule found in the config.
        """
        data_dags = self.parse_data_dags()
        task_dags = self.parse_tasks()
        action_dags, condition_dags = self.parse_actions_and_conditions()
        logger.info(f"Action tasks: {action_dags}, Condition tasks: {condition_dags}")
        dag_tasks = self.generate_task_dependencies(data_dags, task_dags, condition_dags, action_dags)
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
