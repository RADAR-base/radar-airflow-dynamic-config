from pyexpat.errors import messages
from typing import Dict
from dagloader.taskprocessor.taskprocessor import TaskProcessor
import json
import logging
from datetime import datetime, timedelta
import pandas as pd
import os
from typing import List, Any

logger = logging.getLogger(__name__)


class MissingDataTaskProcessor(TaskProcessor):
    def __init__(self, intermediate_storage):
        self.intermediate_storage = intermediate_storage
        super().__init__()

    def execute(self, data, **kwargs) -> Any:
        reports = []
        logger.info(f"Processing missing data task with data: {data}")
        dfs = []
        for data_sources in data.values():
            for topic, topic_data in data_sources.items():
                logger.info(f"Processing topic: {topic}, data: {topic_data}")
                df = pd.DataFrame(topic_data)
                logger.info(f"Data converted to DataFrame: {df}")
                dfs.append(df)
        df = pd.concat(dfs, ignore_index=True)
        logger.info(f"Concatenated DataFrame: {df}")
        logger.info(f"Data converted to DataFrame: {df}")
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        reports = df[df['timestamp'] - pd.to_datetime(
            datetime.utcnow().isoformat()) < timedelta(minutes=15)
                     ]['user_id'].tolist()
        logger.info(f"Collected {len(reports)} reports for topic: {topic}")
        logger.info(f"Report: {reports}")
        report_dict = {}
        report_dict['participants_id'] = reports
        return report_dict