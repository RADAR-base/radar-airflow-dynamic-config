from dagloader.datareader.datareader import DataReader
from airflow.providers.apache.kafka.operators.consume import (
    ConsumeFromTopicOperator
)
import json
from typing import List, Dict, Any

import logging
logger = logging.getLogger(__name__)


class KafkaDataReader(DataReader):
    def __init__(self, conn_id: str, topics: list, max_messages=1000, poll_timeout=5):
        self.conn_id = conn_id
        self.topics = topics
        self.max_messages = max_messages
        self.poll_timeout = poll_timeout

    def get_reader_task(self, topic):
        safe_topic_name = topic.replace('-', '_')
        # Consume from Kafka using provider operator
        consume_task = ConsumeFromTopicOperator(
            task_id=f"consume_{safe_topic_name}",
            topics=[topic],
            apply_function_batch=self.process_consumed_messages,
            apply_function_kwargs={'topic': topic},
            kafka_config_id=self.conn_id,
            max_messages=self.max_messages,
            poll_timeout=self.poll_timeout,
        )
        return consume_task

    def get_reader_tasks(self):
        tasks = {}
        for topic in self.topics:
            task = self.get_reader_task(topic)
            tasks[topic] = task
        return tasks

    @staticmethod
    def process_consumed_messages(messages: List, topic: str) -> Dict[str, Any]:
        logger.info(f"Processing {len(messages)} messages from topic: {topic}")
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
        return message_values