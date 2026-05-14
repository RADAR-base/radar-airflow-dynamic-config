from datetime import datetime, timedelta
import logging
from typing import Dict, List, Tuple, Any

from airflow import DAG

from dagloader.configloader import ConfigLoader
from dagloader.parammaker import ParamMaker
from dagloader.datareader.datareaderoperator import DataReaderOperator
from dagloader.taskprocessor.taskoperator import TaskOperator
from dagloader.intermediatestorage.storagefactory import StorageFactory
from dagloader.conditionparser import ConditionOperator
from dagloader.actionparser import ActionOperator
from dagloader.sensors.sensorfactory import SensorFactory

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# A node in the DAG: the Airflow operator plus the names it depends on.
Node = Tuple[Any, List[str]]


class DAGMaker:
    """Build an Airflow DAG from a YAML config.

    The config defines four kinds of nodes — `sensor`, `data`, `tasks`,
    `actions` — each as a flat list of entries that share the same shape:

        - name: <unique node id>
          type: <factory key>
          enabled: <bool, optional, default true>
          config: { ... }
          depends_on: [<other node name>, ...]

    Because every node references its upstreams by name through
    `depends_on`, the dependency graph is built in a single pass over a
    flat `name -> (operator, depends_on)` map.
    """

    def __init__(self, config_path: str):
        self.config = ConfigLoader(config_path).get_config()

        storage_config = self.config.get('intermediate_results_storage', {})
        self.storage = StorageFactory.get_storage(
            storage_type=storage_config.get('type', 'local'),
            **storage_config.get('config', {}),
        )
        self.model_name = self.config.get('model_name', 'unknown').lower()
        self.storage.init(directory_name=self.model_name)
        self.param_maker = ParamMaker(self.config)

    @staticmethod
    def _is_enabled(entry: Dict) -> bool:
        return entry.get('enabled', True) is not False

    @staticmethod
    def _depends_on(entry: Dict) -> List[str]:
        return list(entry.get('depends_on', []) or [])

    def _build_sensors(self) -> Dict[str, Node]:
        nodes: Dict[str, Node] = {}
        for sensor in self.config.get('sensor', []) or []:
            if not self._is_enabled(sensor):
                continue
            name, sensor_type = sensor.get('name'), sensor.get('type')
            if not name or not sensor_type:
                logger.warning(f"Skipping sensor with invalid config: {sensor}")
                continue
            operator = SensorFactory.get_sensor(
                sensor_type=sensor_type,
                sensor_config=sensor.get('config', {}),
                intermediate_storage=self.storage,
                task_id=name,
            )
            nodes[name] = (operator, self._depends_on(sensor))
        return nodes

    def _build_data(self) -> Dict[str, Node]:
        nodes: Dict[str, Node] = {}
        data_entries = self.config.get('data', []) or []
        for source in data_entries:
            if not self._is_enabled(source):
                continue
            name = source.get('name')
            if not name:
                logger.warning(f"Skipping data source with invalid config: {source}")
                continue
            operator = DataReaderOperator(
                task_id=name,
                data_config=data_entries,
                source_config=source,
                intermediate_storage=self.storage,
            )
            nodes[name] = (operator, self._depends_on(source))
        return nodes

    def _build_tasks(self) -> Dict[str, Node]:
        nodes: Dict[str, Node] = {}
        for task in self.config.get('tasks', []) or []:
            if not self._is_enabled(task):
                continue
            name = task.get('name')
            if not name:
                logger.warning(f"Skipping task with invalid config: {task}")
                continue
            operator = TaskOperator(
                task_id=name,
                task_config=task,
                intermediate_storage=self.storage,
            )
            nodes[name] = (operator, self._depends_on(task))
        return nodes

    def _build_actions(self) -> Dict[str, Node]:
        """Build action nodes, inserting a branch node when a condition is set.

        Conditional flow becomes: <upstreams> -> <name>_condition -> <name>.
        Unconditional flow stays:  <upstreams> -> <name>.
        """
        nodes: Dict[str, Node] = {}
        for action in self.config.get('actions', []) or []:
            if not self._is_enabled(action):
                continue
            name = action.get('name')
            if not name:
                logger.warning(f"Skipping action with invalid config: {action}")
                continue

            upstreams = self._depends_on(action)
            action_op = ActionOperator(
                task_id=name,
                action_config=action,
                intermediate_storage=self.storage,
            )

            if action.get('condition'):
                branch_name = f"{name}_condition"
                branch_op = ConditionOperator(
                    task_id=f"{name}_branch",
                    action_config=action,
                    intermediate_storage=self.storage,
                )
                nodes[branch_name] = (branch_op, upstreams)
                nodes[name] = (action_op, [branch_name])
            else:
                nodes[name] = (action_op, upstreams)
        return nodes

    @staticmethod
    def _wire(nodes: Dict[str, Node]) -> None:
        for name, (operator, depends_on) in nodes.items():
            for dep in depends_on:
                upstream = nodes.get(dep)
                if upstream is None:
                    logger.warning(f"Dependency '{dep}' not found for '{name}'")
                    continue
                upstream[0] >> operator

    def build_nodes(self) -> Dict[str, Node]:
        nodes = {
            **self._build_sensors(),
            **self._build_data(),
            **self._build_tasks(),
            **self._build_actions(),
        }
        self._wire(nodes)
        logger.info(f"Built DAG nodes: {list(nodes.keys())}")
        return nodes

    def generate_dags(self) -> DAG:
        dag_id = self.model_name
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
        dag_params = self.param_maker.generate_params()
        logger.info(
            f"Creating DAG '{dag_id}' schedule={dag_schedule} params={dag_params}"
        )
        with DAG(
            dag_id=dag_name,
            default_args=default_args,
            description=self.config.get('model_description', ''),
            schedule=dag_schedule,
            start_date=datetime(2024, 1, 1),
            catchup=False,
            tags=['radar', 'dynamic', 'python-class'],
            params=dag_params,
        ) as dag:
            self.build_nodes()
        return dag
