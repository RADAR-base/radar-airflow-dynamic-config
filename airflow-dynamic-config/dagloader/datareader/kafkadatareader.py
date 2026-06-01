from dagloader.datareader.datareader import DataReader
from airflow.providers.apache.kafka.hooks.consume import KafkaConsumerHook
import json
from typing import List, Dict, Any
import time


import logging
logger = logging.getLogger(__name__)


class KafkaDataReader(DataReader):
    def __init__(self, conn_id: str, topics: list, max_messages=1000,
                 poll_timeout=5, format='json', lookback_window=None,
                 schema_registry_url=None):
        self.conn_id = conn_id
        self.topics = topics if isinstance(topics, list) else [topics]
        self.max_messages = max_messages
        self.poll_timeout = poll_timeout
        self.format = format
        self.lookback_window = lookback_window
        self.schema_registry_url = schema_registry_url
        self._avro_deserializer = None

        if self.format == 'avro' and not self.schema_registry_url:
            raise ValueError(
                "schema_registry_url is required when format is 'avro'"
            )

    def _get_avro_deserializer(self):
        """Lazily build a Confluent Avro deserializer backed by the schema
        registry. Avro messages on the topic are produced in Confluent wire
        format (magic byte + schema id + payload), so the writer schema is
        resolved from the registry at decode time."""
        if self._avro_deserializer is None:
            from confluent_kafka.schema_registry import SchemaRegistryClient
            from confluent_kafka.schema_registry.avro import AvroDeserializer

            schema_registry_client = SchemaRegistryClient(
                {"url": self.schema_registry_url}
            )
            self._avro_deserializer = AvroDeserializer(schema_registry_client)
        return self._avro_deserializer

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

    def _decode_value(self, msg, topic):
        """Decode a single message value according to the configured format.

        Returns the decoded value, or None if there is nothing to decode."""
        value = msg.value()
        if value is None:
            return None
        if self.format == 'json':
            if isinstance(value, bytes):
                value = value.decode('utf-8')
            return json.loads(value)
        elif self.format == 'avro':
            from confluent_kafka.serialization import (
                MessageField,
                SerializationContext,
            )
            return self._get_avro_deserializer()(
                value, SerializationContext(topic, MessageField.VALUE)
            )
        else:
            logger.warning(f"Unsupported format '{self.format}' for topic {topic}")
            return None

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
                    if not self._check_lookback_window(msg.timestamp()[1] / 1000.0):
                        # commit the message to avoid reprocessing in future runs
                        consumer.commit(message=msg)
                        continue
                    value = self._decode_value(msg, topic)
                    if value is not None:
                        message_values.append(value)
                except Exception as e:
                    logger.warning(f"Error processing message from {topic}: {e}")
                    continue
            logger.info(
                f"Consumed {len(message_values)} messages from topic: {topic}"
            )
        finally:
            consumer.close()
        return message_values
