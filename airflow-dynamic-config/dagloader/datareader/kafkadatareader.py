from dagloader.datareader.datareader import DataReader
from airflow.providers.apache.kafka.hooks.consume import KafkaConsumerHook
import json
from typing import List, Dict, Any
import time


import logging
logger = logging.getLogger(__name__)


class KafkaDataReader(DataReader):
    def __init__(self, conn_id: str, topics: list, max_messages=1000,
                 poll_timeout=5, format='json', lookback_window=None):
        self.conn_id = conn_id
        self.topics = topics if isinstance(topics, list) else [topics]
        self.max_messages = max_messages
        self.poll_timeout = poll_timeout
        self.format = format
        self.lookback_window = lookback_window

    def read_data(self):
        logger.info(f"Reading data from Kafka topics: {self.topics}")
        data = {}
        for topic in self.topics:
            logger.info(f"Consuming messages from topic: {topic}")
            data[topic] = self._consume_topic(topic)
        return data

    def _check_lookback_window(self, msg_timestamp):
        if self.lookback_window is None:
            return True
        current_time = time.time()
        return msg_timestamp >= current_time - self.lookback_window

    def _consume_topic(self, topic: str) -> List[Dict[str, Any]]:
        """Consume messages from a single Kafka topic using KafkaConsumerHook."""
        hook = KafkaConsumerHook(
            topics=[topic],
            kafka_config_id=self.conn_id,
        )
        consumer = hook.get_consumer()
        message_values = []
        try:
            messages = consumer.consume(
                num_messages=self.max_messages,
                timeout=self.poll_timeout,
            )
            for msg in messages:
                if msg.error():
                    logger.warning(f"Consumer error on topic {topic}: {msg.error()}")
                    continue
                try:
                    if self.format == 'json':
                        value = msg.value()
                        if value is not None:
                            if isinstance(value, bytes):
                                value = value.decode('utf-8')
                            if self._check_lookback_window(msg.timestamp()[1] / 1000.0):
                                message_values.append(json.loads(value))
                            else:
                                # commit the message to avoid reprocessing in future runs
                                consumer.commit(message=msg)
                    elif self.format == 'avro':
                        value = msg.value()
                        if value is not None:
                            if self._check_lookback_window(msg.timestamp()[1] / 1000.0):
                                message_values.append(value)
                            else:
                                consumer.commit(message=msg)
                    else:
                        logger.warning(f"Unsupported format '{self.format}' for topic {topic}")
                except Exception as e:
                    logger.warning(f"Error processing message from {topic}: {e}")
                    continue
            logger.info(
                f"Consumed {len(message_values)} messages from topic: {topic}"
            )
        finally:
            consumer.close()
        return message_values
