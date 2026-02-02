import sys
import os

from datetime import datetime, timedelta
import glob
import hashlib
from typing import Dict, Any

# Add the current directory to Python path to allow imports
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

from dagloader.dagmaker import DAGMaker
import logging

logger = logging.getLogger(__name__)

def load_all_dags():
    """Load all DAGs from config files in the configs directory."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    configs_dir = os.path.join(current_dir, 'configs')
    #config_files = glob.glob(os.path.join(configs_dir, '*.yaml'))
    config_files = glob.glob(os.path.join(configs_dir, 'sample_config_missing_data.yaml'))
    logger.info(f"Looking for config files in: {configs_dir}")
    logger.info(f"Found config files: {config_files}")
    dags_dict = {}
    for config_file in config_files:
        try:
            dag_maker = DAGMaker(config_file)
            dags_list = dag_maker.generate_dags()
            # Add each DAG to the dictionary with its dag_id as the key
            for dag in dags_list:
                dags_dict[dag.dag_id] = dag
                logger.info(f"Loaded DAG: {dag.dag_id}")
        except Exception as e:
            logger.error(f"Error parsing config {config_file}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
    return dags_dict


# Create all DAGs and add them to module globals
# This is required for Airflow to discover the DAGs
_all_dags = load_all_dags()
globals().update(_all_dags)


if __name__ == "__main__":
    dags_dict = load_all_dags()
    logger.info(f"Generated DAGs: {list(dags_dict.keys())}")
