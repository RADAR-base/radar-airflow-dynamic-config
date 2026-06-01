from dagloader.datareader.datareader import DataReader
from airflow.providers.apache.kafka.hooks.consume import KafkaConsumerHook
from confluent_kafka import TIMESTAMP_NOT_AVAILABLE
import json
from typing import List, Dict, Any
import time


import logging
logger = logging.getLogger(__name__)


class KafkaDataReader(DataReader):
    def __init__(self, conn_id: str, topics: list, max_messages=1000,
                 poll_timeout=5, format='json', lookback_window=None,
                 schema_registry_url=None, group_id=None):
        self.conn_id = conn_id
        self.topics = topics if isinstance(topics, list) else [topics]
        self.max_messages = max_messages
        self.poll_timeout = poll_timeout
        self.format = format
        self.lookback_window = lookback_window
        self.schema_registry_url = schema_registry_url
        # When set, overrides the connection's group.id so each DAG consumes
        # under its own group and does not share/steal offsets with others.
        self.group_id = group_id
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

    def _is_outside_lookback(self, msg) -> bool:
        """True when the message is older than the lookback window and should
        be skipped (and its offset committed). Messages without a usable
        timestamp are treated as in-window so they are never silently dropped.
        """
        if self.lookback_window is None:
            return False
        ts_type, ts = msg.timestamp()
        if ts_type == TIMESTAMP_NOT_AVAILABLE or ts < 0:
            return False
        return not self._check_lookback_window(ts / 1000.0)

    def _get_consumer(self, topic: str):
        """Build a subscribed consumer for a single topic.

        When ``group_id`` is set we override the connection's ``group.id`` so
        each DAG runs under its own consumer group. We reuse the hook's
        ``_get_client`` so the provider's error callback (and any auth
        handling baked into the config) is preserved."""
        hook = KafkaConsumerHook(topics=[topic], kafka_config_id=self.conn_id)
        if not self.group_id:
            return hook.get_consumer()
        config = hook.get_connection(self.conn_id).extra_dejson
        config["group.id"] = self.group_id
        consumer = hook._get_client(config)
        consumer.subscribe([topic])
        return consumer

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
        """Consume messages from a single Kafka topic.

        Polls in a loop until ``max_messages`` are collected or the poll
        budget is exhausted. A single ``consume()`` on a freshly subscribed
        consumer is unreliable: the first poll(s) are spent on the
        consumer-group rebalance / partition assignment and return no records,
        so a one-shot consume-then-close reads nothing even when data exists.
        """
        consumer = self._get_consumer(topic)
        message_values = []
        # Give a cold consumer enough time to join the group and be assigned
        # partitions before the budget runs out.
        deadline = time.monotonic() + max(self.poll_timeout, 10)
        try:
            while len(message_values) < self.max_messages:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                messages = consumer.consume(
                    num_messages=self.max_messages - len(message_values),
                    timeout=min(1.0, remaining),
                )
                for msg in messages:
                    value = self._handle_message(consumer, msg, topic)
                    if value is not None:
                        message_values.append(value)
            logger.info(
                f"Consumed {len(message_values)} messages from topic: {topic}"
            )
        finally:
            consumer.close()
        return message_values

    def _handle_message(self, consumer, msg, topic):
        """Validate, lookback-filter and decode a single message.

        Returns the decoded value, or None when the message is an error,
        outside the lookback window (committed and skipped), or fails to
        decode."""
        if msg.error():
            logger.warning(f"Consumer error on topic {topic}: {msg.error()}")
            return None
        try:
            if self._is_outside_lookback(msg):
                # Too old to process: commit synchronously so the offset
                # advances past it and it is not re-read on the next run.
                consumer.commit(message=msg, asynchronous=False)
                return None
            return self._decode_value(msg, topic)
        except Exception as e:
            logger.warning(f"Error processing message from {topic}: {e}")
            return None
