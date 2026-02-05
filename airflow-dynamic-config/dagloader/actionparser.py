from airflow.providers.apache.kafka.operators.produce import (
    ProduceToTopicOperator
)
from airflow.sdk import BaseOperator

class ActionParser:
    def __init__(self, actions: dict, output_topic: str = "output_topic", kafka_conn_id: str = "kafka_default"):
        self.actions = actions
        self.OUTPUT_TOPIC = output_topic
        self.KAFKA_CONN_ID = kafka_conn_id

    def parse_actions(self):
        parsed_actions = {}
        for action_name, action_details in self.actions.items():
            parsed_actions[action_name] = self.parse_single_action(action_name, action_details)
        return parsed_actions

    def parse_single_action(self, action_name: str, action_details: dict):
        # Implement parsing logic here
        producer_function = self.get_producer_function(action_details)
        return  self.get_action_tasks(action_name, action_details, producer_function)

    def get_action_tasks(self, action_name, action_details, producer_function):
        publish_task = ProduceToTopicOperator(
            task_id=action_name,
            topic=self.OUTPUT_TOPIC,
            kafka_config_id=self.KAFKA_CONN_ID,
            producer_function=producer_function,
        )
        return publish_task

    def get_producer_function(self, action_details):
        def producer_function(**context):
            # Example producer function logic
            report = {"action": action_details.get("type", "default_action"), "status": "completed"}
            return report
        return producer_function


class ActionOperator(BaseOperator):
    def __init__(self, actions: dict, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.action_parser = ActionParser(actions)
        self.parsed_actions = self.action_parser.parse_actions()

    def execute(self, context):
        for action_name, action_task in self.parsed_actions.items():
            action_task.execute(context)