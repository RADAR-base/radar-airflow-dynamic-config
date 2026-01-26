from dagloader.datareader.datareader import DataReader
from airflow.providers.apache.kafka.operators.consume import (
    ConsumeFromTopicOperator
)
import logging
logger = logging.getLogger(__name__)


class KafkaDataReader(DataReader):
    def __init__(self, conn_id: str, topics: list, max_messages=1000, poll_timeout=5):
        self.conn_id = conn_id
        self.topics = topics
        self.max_messages = max_messages
        self.poll_timeout = poll_timeout

    def get_reader_task(self, apply_function_batch, topic):
        safe_topic_name = topic.replace('-', '_')
        # Consume from Kafka using provider operator
        consume_task = ConsumeFromTopicOperator(
            task_id=f"consume_{safe_topic_name}",
            topics=[topic],
            apply_function_batch=apply_function_batch,
            apply_function_kwargs={'topic': topic},
            kafka_config_id=self.conn_id,
            max_messages=self.max_messages,
            poll_timeout=self.poll_timeout,
        )
        return consume_task

    def get_reader_tasks(self, apply_function_batch):
        tasks = {}
        for topic in self.topics:
            task = self.get_reader_task(apply_function_batch, topic)
            tasks[topic] = task
        return tasks