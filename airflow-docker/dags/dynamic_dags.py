from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import yaml
import glob
import os
import hashlib
from typing import Dict, Any


def load_config(config_path: str) -> Dict[str, Any]:
    """Load and parse a YAML configuration file."""
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)


def create_task_function(task_config: Dict[str, Any]):
    """Create a function that will be executed by the task."""
    def task_function(**context):
        task_type = task_config.get('type')
        if task_type == 'inference':
            # Implement inference logic
            print(f"Running inference task with config: {task_config}")
        elif task_type == 'training':
            # Implement training logic
            print(f"Running training task with config: {task_config}")
        elif task_type == 'data_checks':
            # Implement data checks logic
            print(f"Running data checks with config: {task_config}")
        elif task_type == 'custom':
            # Implement custom script logic
            print(f"Running custom script with config: {task_config}")
            # Here you would typically import and run the script from task_config['path']
        else:
            raise ValueError(f"Unknown task type: {task_type}")
    return task_function


def create_dag_from_config(config_path: str) -> DAG:
    """Create an Airflow DAG from a configuration file."""
    config = load_config(config_path)
    # Extract the model name from config to use as DAG ID
    dag_id = f"radar_{config.get('model_name', 'unknown').lower()}"
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }


    # This function remains to support single-config DAG creation if needed,
    # but it will create a DAG that contains all tasks from the config without
    # grouping by schedule. The main loader below will create grouped DAGs.
    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=config.get('model_description', ''),
        schedule=None,
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=['radar', 'dynamic'],
    ) as dag:

        tasks = {}
        # First pass: Create all tasks
        for task_config in config.get('tasks', []):
            if not task_config.get('enabled', True):  # Skip disabled tasks
                continue
            task_name = task_config['task_name']
            # create operator
            task = PythonOperator(
                task_id=task_name,
                python_callable=create_task_function(task_config),
                dag=dag,
            )
            tasks[task_name] = task

        # Second pass: Set up task dependencies
        for task_config in config.get('tasks', []):
            task_name = task_config['task_name']
            if task_name in tasks and 'depends_on' in task_config and task_config.get('depends_on'):
                upstream_task = task_config['depends_on']
                if upstream_task in tasks:
                    tasks[upstream_task] >> tasks[task_name]
        return dag


def load_all_dags():
    """Load all DAGs from config files in the configs directory."""
    # Get the directory where this file is located
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Construct the path to the configs directory
    configs_dir = os.path.join(current_dir, 'configs')
    # Get all yaml files
    config_files = glob.glob(os.path.join(configs_dir, '*.yaml'))
    print(f"Looking for config files in: {configs_dir}")
    print(f"Found config files: {config_files}")
    # Parse all configs first
    parsed_configs = []
    for config_file in config_files:
        try:
            cfg = load_config(config_file)
            cfg['_config_path'] = config_file
            parsed_configs.append(cfg)
        except Exception as e:
            print(f"Error parsing config {config_file}: {str(e)}")

    # Map schedule -> list of (config, end_task_name)
    schedule_map = {}
    for cfg in parsed_configs:
        model = cfg.get('model_name', 'unknown').lower()
        for task in cfg.get('tasks', []):
            if not task.get('enabled', True):
                continue
            schedule = task.get('schedule')
            if schedule:
                # treat any task that specifies a schedule as an end-task
                schedule_map.setdefault(schedule, []).append((cfg, task['task_name']))

    dags = []
    # Helper to sanitize model names for use as task prefixes
    def _sanitize(name: str) -> str:
        return ''.join(ch if ch.isalnum() else '_' for ch in name.lower())

    # For each schedule, create a DAG containing all end-tasks with that schedule
    for schedule, end_tasks in schedule_map.items():
        # Make a short hash for the schedule to keep part of the id stable
        schedule_hash = hashlib.md5(schedule.encode()).hexdigest()[:8]

        # If this schedule group contains exactly one end-task, name the DAG
        # using the model name followed by the task name: <model>__<task>
        # (sanitized). If that collides, append the schedule hash.
        if len(end_tasks) == 1:
            only_cfg, only_task_name = end_tasks[0]
            model_name = only_cfg.get('model_name', 'unknown')
            candidate = f"{_sanitize(model_name)}__{_sanitize(only_task_name)}"
            existing_ids = {d.dag_id for d in dags}
            dag_id = candidate if candidate not in existing_ids else f"{candidate}_{schedule_hash}"
            involved_models = [model_name]
            description = (
                f"DAG for end-task '{only_task_name}' on model {model_name} with schedule {schedule}"
            )
        else:
            # Multiple end-tasks -> composite id built from models and task names
            raw_sched = ''.join(ch if ch.isalnum() else '_' for ch in schedule)
            short_sched = raw_sched[:30].strip('_') or schedule_hash
            model_name = cfg.get('model_name', 'unknown')
            involved_models = sorted({cfg.get('model_name', 'unknown') for cfg, _ in end_tasks})
            sanitized_models = [_sanitize(m) for m in involved_models]
            if len(sanitized_models) == 0:
                models_part = 'unknown'
            elif len(sanitized_models) == 1:
                models_part = sanitized_models[0][:30]
            elif len(sanitized_models) == 2:
                models_part = (sanitized_models[0] + '__' + sanitized_models[1])[:40]
            else:
                models_part = (
                    sanitized_models[0] + '__' + sanitized_models[1]
                )[:36] + f"__{len(sanitized_models)}m"

            # Compose task names part (unique end-task names in this group)
            end_task_names = [tname for _, tname in end_tasks]
            sanitized_tasks = [_sanitize(t) for t in end_task_names]
            tasks_part = '__'.join(sanitized_tasks)[:60]

            dag_id = f"{models_part}__{tasks_part}_{schedule_hash}"
            description = (
                f"Grouped DAG for schedule '{schedule}' covering models: {', '.join(involved_models)}"
            )

        default_args = {
            'owner': 'airflow',
            'depends_on_past': False,
            'email_on_failure': True,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        }

        with DAG(
            dag_id=dag_id,
            default_args=default_args,
            description=description,
            schedule=schedule,
            start_date=datetime(2024, 1, 1),
            catchup=False,
            tags=['radar', 'dynamic', 'grouped', model_name],
        ) as dag:

            operators = {}

            # Collect all tasks needed for this DAG (end-tasks and their upstream deps)
            tasks_to_include = {}

            def _collect_upstream(cfg, task_name):
                # cfg is the config dict containing tasks
                tasks = {t['task_name']: t for t in cfg.get('tasks', []) if t.get('enabled', True)}
                if task_name not in tasks:
                    return
                stack = [task_name]
                while stack:
                    tn = stack.pop()
                    key = (cfg.get('model_name', 'unknown').lower(), tn)
                    if key in tasks_to_include:
                        continue
                    tasks_to_include[key] = tasks[tn]
                    depends = tasks[tn].get('depends_on')
                    if depends:
                        stack.append(depends)

            for cfg, end_task_name in end_tasks:
                _collect_upstream(cfg, end_task_name)

            # Create operators with prefixed task_ids to avoid collisions across models
            for (model, task_name), task_cfg in tasks_to_include.items():
                prefix = _sanitize(model)
                prefixed_task_id = f"{prefix}__{task_name}"
                operators[(model, task_name)] = PythonOperator(
                    task_id=prefixed_task_id,
                    python_callable=create_task_function(task_cfg),
                    dag=dag,
                )

            # Wire dependencies using the original 'depends_on' fields
            for (model, task_name), task_cfg in tasks_to_include.items():
                depends = task_cfg.get('depends_on')
                if depends:
                    upstream_key = (model, depends)
                    if upstream_key in operators and (model, task_name) in operators:
                        operators[upstream_key] >> operators[(model, task_name)]

            dags.append(dag)

    print(f"Created {len(dags)} grouped DAG(s) based on end-task schedules")
    return dags


# Create all DAGs
globals().update(
    {dag.dag_id: dag for dag in load_all_dags()}
)


if __name__ == "__main__":
    dags = load_all_dags()
    print({dag.dag_id: dag for dag in dags})
