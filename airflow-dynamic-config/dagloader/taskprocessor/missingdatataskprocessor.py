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
    def __init__(self):
        super().__init__()

    @staticmethod
    def processor_task(**kwargs) -> Any:
        intermediate_storage = kwargs.get('intermediate_storage')
        data_key = kwargs.get('source_name')
        data = intermediate_storage.load(data_key)
        reports = []
        df = pd.DataFrame(data)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        reports = df[df['timestamp'] - pd.to_datetime(
            datetime.utcnow().isoformat()) < timedelta(minutes=15)
                     ]['user_id'].tolist()
        logger.info(f"Collected {len(reports)} reports for topic: {topic}")
        logger.info(f"Report: {reports}")
        return reports

    def get_processor_task(self) -> Any:
        return self.processor_task

    def get_data_processor_task(self) -> Any:
        return self.process_consumed_messages

    @staticmethod
    def process_consumed_messages(messages: List, topic: str) -> Dict[str, Any]:
        logger.info(f"Processing {len(messages)} messages from topic: {topic}")
        INTERMEDIATE_PATH = "/tmp/missing_data_intermediate"
        # Extract message values
        message_values = []
        for msg in messages:
            try:
                if isinstance(msg, dict):
                    message_values.append(msg)
                elif hasattr(msg, 'value'):
                    value = msg.value()
                    if isinstance(value, (str, bytes)):
                        message_values.append(json.loads(value))
                    else:
                        message_values.append(value)
            except Exception as e:
                logger.warning(f"Error processing message: {e}")
                continue

        df = pd.DataFrame(message_values)
        total_messages = len(df)
        logger.info(f"Total messages for analysis: {total_messages}")
        logger.info(f"DataFrame columns: {df.head()}")
        df_last_data_point = df.groupby("user_id")['timestamp'].max().reset_index()
        logger.info(f"DataFrame grouped by user_id: {df_last_data_point.head()}")
        if os.path.exists(INTERMEDIATE_PATH):
            if os.path.exists(os.path.join(INTERMEDIATE_PATH, f"last_data_point_{topic}.csv")):
                df_previous = pd.read_csv(os.path.join(
                    INTERMEDIATE_PATH,
                    f"last_data_point_{topic}.csv"
                ))
                logger.info(f"Previous DataFrame loaded: {df_previous.head()}")
                ## Update the last data points with the most recent timestamps
                df_last_data_point = pd.concat(
                    [df_previous, df_last_data_point]
                ).groupby("user_id")['timestamp'].max().reset_index()
                logger.info(f"Updated last data points DataFrame: {df_last_data_point.head()}")
            df_last_data_point.to_csv(
                os.path.join(
                    INTERMEDIATE_PATH,
                    f"last_data_point_{topic}.csv"
                ),index=False)
            logger.info(f"directory exists: {os.listdir(INTERMEDIATE_PATH)}")
