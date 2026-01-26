from datetime import datetime, timedelta
import logging
from dagloader.configloader import ConfigLoader
from airflow import DAG
from dagloader.datareader.kafkadatareader import KafkaDataReader
from dagloader.taskprocessor.missingdatataskprocessor import MissingDataTaskProcessor
from typing import Dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DAGMaker:
    def __init__(self, config_path: str):
        self.config_loader = ConfigLoader(config_path)
        self.config = self.config_loader.get_config()

    def generate_task_dependencies(self, data_dags: Dict, task_dags: Dict) -> Dict:
        """
        This function will generate task dependencies based on data DAGs and task DAGs.
        It will return a dictionary of DAGs with their tasks and dependencies.
        Algo:
        """
        dag_tasks = {}
        for task_config in self.config.get('tasks', []):
            logger.info(f"Processing task config: {task_config}")
            schedule = task_config.get('schedule')
            is_enabled = task_config.get('enabled', True)
            if not is_enabled:
                continue
            task_name = task_config.get('task_name')
            task_instance = task_dags.get(task_name)
            if task_instance is None:
                continue
            for data_source in task_config.get('data_sources', []):
                logger.info(f"Linking data source: {data_source} to task: {task_name}")
                data_dag = data_dags.get(data_source)
                logger.info(f"Found data DAG: {data_dag} for source: {data_source}")
                if data_dag is None:
                    continue
                data_processor_task = task_instance.get_data_processor_task()
                logger.info(f"Generated data processor task: {data_processor_task} for task: {task_name}")
                reader_tasks = data_dag.get_reader_tasks(
                    apply_function_batch=data_processor_task
                )
                logger.info(f"Generated reader tasks: {reader_tasks} for data source: {data_source}")
                for reader_task in reader_tasks.values():
                    dag_key = f"{task_name}_{data_source}"
                    if dag_key not in dag_tasks:
                        dag_tasks[dag_key] = {
                            'name': dag_key,
                            'schedule': schedule,
                            'tasks': []
                        }
                    dag_tasks[dag_key]['tasks'].append(
                        [reader_task, task_instance.get_processor_task()])
            # generate DAGs
        logging.info(f"Generated DAG tasks: {dag_tasks}")
        return dag_tasks

    def parse_source_types(self, data_configs: Dict) -> Dict:
        """
        This function will parse the source types from the config.
        It will return a dictionary of source types with their configurations.
        """
        source_types = data_configs['source_types']
        data_reader_tasks = {}
        for source in source_types:
            if source['type'] == 'kafka':
                data_reader_tasks[source['name']] = KafkaDataReader(
                    conn_id=source['source_config']['conn_id'],
                    topics=source['source_config']['topics'],
                    max_messages=source.get('max_messages', 1000),
                    poll_timeout=source.get('poll_timeout', 5)
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
            if task['type'] == 'data_checks':
                tasks_dags[task['task_name']] = MissingDataTaskProcessor()
            else:
                # Handle other task types
                pass
        logger.info(f"Parsed task DAGs: {tasks_dags}")
        return tasks_dags

    def parse_actions(self) -> Dict:
        """
        This function will parse the actions from the config.
        It will return a dictionary of actions with their configurations.
        """
        task_configs = self.config.get('tasks', [])
        action_dags = {}
        for task in task_configs:
            for action in task.get('actions', []):
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
        #action_tasks = self.parse_actions()
        dag_tasks = self.generate_task_dependencies(data_dags, task_dags)
        return dag_tasks

    def generate_dags(self):
        # Logic to create DAG based on self.config
        dag_id = f"{self.config.get('model_name', 'unknown').lower()}"
        default_args = {
            'owner': 'airflow',
            'depends_on_past': False,
            'email_on_failure': True,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        }
        dag_tasks = self.parse_configs()
        dags = []
        for dag_name, dag_dict in dag_tasks.items():
            with DAG(
                dag_id=dag_name,
                default_args=default_args,
                description=self.config.get('model_description', ''),
                schedule=dag_dict['schedule'],
                start_date=datetime(2024, 1, 1),
                catchup=False,
                tags=['radar', 'dynamic', 'python-class'],
            ) as dag:
                for task_chain in dag_dict.get('tasks', []):
                    current_task = None
                    for task in task_chain:
                        logger.info(f"Adding task: {task} to DAG: {dag_name}")
                        if current_task is not None:
                            current_task >> task()
                        current_task = task
            dags.append(dag)
        return dags
