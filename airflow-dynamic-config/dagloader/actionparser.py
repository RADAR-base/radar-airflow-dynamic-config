from airflow.providers.apache.kafka.operators.produce import (
    ProduceToTopicOperator
)
from airflow.sdk import BaseOperator


class ActionParser:
    def __init__(self, actions: dict, output_topic: str = "output_topic", kafka_conn_id: str = "kafka_default"):
        self.actions = actions
        self.action_name = actions.get('name', 'default_action')
        self.action_type = actions.get('type', 'default_type')
        self.action_config = actions.get('config', {})
        self.OUTPUT_TOPIC = output_topic
        self.KAFKA_CONN_ID = kafka_conn_id

    def parse_action(self):
        return self.get_action_tasks(self.action_name, self.action_config)

    def get_action_tasks(self, data):
        publish_task = ProduceToTopicOperator(
            task_id=f"{self.action_name}_publish",
            topic=self.OUTPUT_TOPIC,
            kafka_config_id=self.KAFKA_CONN_ID,
            producer_function=self.get_producer_function(self.action_type,
                                                         self.action_config)
        )
        return publish_task

    @staticmethod
    def get_producer_function(action_type, action_config):
        def producer_function(data):
            # Example producer function logic
            report = {"action": action_config.get("type", "default_action"), "status": "completed"}
            return report
        return producer_function


class ActionOperator(BaseOperator):
    def __init__(self, action_config: dict, intermediate_storage,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.intermediate_storage = intermediate_storage
        self.action_parser = ActionParser(action_config)
        #self.parsed_action = self.action_parser.parse_action()
        self.keys = action_config.get('depends_on', [])

    def execute(self, context):
        data = {}
        for key in self.keys:
            data[key] = self.intermediate_storage.load(key)
        self.parsed_action.get_action_tasks(data)()
