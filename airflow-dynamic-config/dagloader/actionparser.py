from airflow.providers.apache.kafka.operators.produce import (
    ProduceToTopicOperator
)
from airflow.providers.apache.kafka.hooks.produce import KafkaProducerHook
from airflow.sdk import BaseOperator
import logging
logger = logging.getLogger(__name__)

class KafkaWriter():
    def __init__(self, topic: str, kafka_conn_id: str):
        self.topic = topic
        self.kafka_conn_id = kafka_conn_id

    def write(self, data):
        producer_hook = KafkaProducerHook(kafka_config_id=self.kafka_conn_id)
        producer = producer_hook.get_producer()
        try:
            producer.produce(
                topic=self.topic,
                value=data.encode('utf-8') if isinstance(data, str) else data
            )
            producer.flush()
        finally:
            producer.close()


class ActionParser:
    def __init__(self, actions: dict, output_topic: str = "output_topic", 
                 kafka_conn_id: str = "kafka_default"):
        self.actions = actions
        self.action_name = actions.get('name', 'default_action')
        self.action_type = actions.get('type', 'default_type')
        self.action_config = actions.get('config', {})
        self.OUTPUT_TOPIC = output_topic
        self.KAFKA_CONN_ID = kafka_conn_id

    def producer_function(self, data):
        # Example producer function logic
        report = {"action": self.action_config.get("type", "default_action"), "status": "completed"}
        return report


class ActionOperator(BaseOperator):
    def __init__(self, action_config: dict, intermediate_storage,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.intermediate_storage = intermediate_storage
        self.action_parser = ActionParser(action_config)
        self.keys = action_config.get('depends_on', [])
        self.writer = KafkaWriter(
            topic=self.action_parser.OUTPUT_TOPIC,
            kafka_conn_id=self.action_parser.KAFKA_CONN_ID
        )

    def execute(self, context):
        logger.info(f"Executing action: {self.action_parser.action_name} of type: {self.action_parser.action_type}")
        data = {}
        for key in self.keys:
            data[key] = self.intermediate_storage.load(key)
        logger.info(f"Data loaded for action {self.action_parser.action_name}: {data}")
        report = self.action_parser.producer_function(data)
        logger.info(f"Action {self.action_parser.action_name} produced report: {report}")
        self.writer.write(str(report))
        logger.info(f"Report for action {self.action_parser.action_name} sent to Kafka topic: {self.writer.topic}")
