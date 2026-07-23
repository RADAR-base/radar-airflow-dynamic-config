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
        dfs = []
        for data_sources in data.values():
            for topic, topic_data in data_sources.items():
                df = pd.DataFrame(topic_data)
                dfs.append(df)
        df = pd.concat(dfs, ignore_index=True)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        reports = df[df['timestamp'] - pd.to_datetime(
            datetime.utcnow().isoformat()) < timedelta(minutes=15)
                     ]['user_id'].tolist()
        report_dict = {}
        report_dict['participants_id'] = reports
        return report_dict