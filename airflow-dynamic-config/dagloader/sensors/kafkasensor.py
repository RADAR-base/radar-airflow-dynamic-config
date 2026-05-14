from airflow.providers.apache.kafka.hooks.consume import KafkaConsumerHook
from dagloader.sensors.sensoroperator import ConfigurableSensorOperator
import logging

logger = logging.getLogger(__name__)


class KafkaSensorOperator(ConfigurableSensorOperator):
    def _resolve_config(self):
        self.conn_id = self.get_config('conn_id', 'kafka_default')
        topics = self.get_config('topics') or self.get_config('topic')
        if isinstance(topics, list):
            self.topics = topics
        elif topics:
            self.topics = [topics]
        else:
            self.topics = []
        self.max_messages = int(self.get_config('max_messages', 1))
        self.poll_timeout = int(self.get_config('poll_timeout', 5))
        self.consumer_config = self.get_config('consumer_config', {})

    def poke(self, context):
        if not self.topics:
            logger.warning("Kafka sensor has no topics configured.")
            return False

        hook = KafkaConsumerHook(
            topics=self.topics,
            kafka_config_id=self.conn_id,
            consumer_config=self.consumer_config,
        )
        consumer = hook.get_consumer()
        matched_messages = []
        try:
            messages = consumer.consume(
                num_messages=self.max_messages,
                timeout=self.poll_timeout,
            )
            for msg in messages:
                if msg is None:
                    continue
                if msg.error():
                    logger.warning(
                        f"Kafka sensor error on {msg.topic()}: {msg.error()}"
                    )
                    continue
                if msg.value() is not None:
                    matched_messages.append(msg.value())
        finally:
            consumer.close()

        if not matched_messages:
            return False

        self.save_to_storage(matched_messages)
        return True
