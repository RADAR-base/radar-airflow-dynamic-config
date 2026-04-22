from airflow.providers.apache.kafka.hooks.produce import KafkaProducerHook
from airflow.sdk import BaseOperator
import logging
import re
from typing import Any
from dateutil import parser

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
        self.action_time = actions.get('time', 'immediate')
        self.OUTPUT_TOPIC = output_topic
        self.KAFKA_CONN_ID = kafka_conn_id

    TEMPLATE_PATTERN = re.compile(r"\{\{\s*([^{}]+?)\s*\}\}")

    def _resolve_path(self, context: dict, path: str) -> Any:
        current: Any = context
        for part in path.split('.'):
            if isinstance(current, dict) and part in current:
                current = current[part]
            elif isinstance(current, list) and part.isdigit():
                index = int(part)
                if 0 <= index < len(current):
                    current = current[index]
                else:
                    return None
            else:
                return None
        return current

    def _render_template_string(self, value: str, context: dict) -> Any:
        matches = list(self.TEMPLATE_PATTERN.finditer(value))
        if not matches:
            return value

        # Keep native type when the whole value is a single template token.
        if len(matches) == 1 and matches[0].span() == (0, len(value)):
            resolved = self._resolve_path(context, matches[0].group(1).strip())
            if resolved is None:
                logger.warning(
                    f"Failed to resolve template '{value}' in action "
                    f"{self.action_name}."
                )
                return value
            return resolved

        rendered = value
        for match in matches:
            template = match.group(0)
            path = match.group(1).strip()
            resolved = self._resolve_path(context, path)
            replacement = "" if resolved is None else str(resolved)
            rendered = rendered.replace(template, replacement)
        return rendered

    def _render_templates(self, value: Any, context: dict) -> Any:
        if isinstance(value, dict):
            return {
                key: self._render_templates(inner_value, context)
                for key, inner_value in value.items()
            }
        if isinstance(value, list):
            return [self._render_templates(item, context) for item in value]
        if isinstance(value, str):
            return self._render_template_string(value, context)
        return value

    def producer_function(self, data):
        config = (
            self.action_config
            if isinstance(self.action_config, dict)
            else {}
        )
        resolved_config = self._render_templates(config, data)
        # try convering time to dateti
        time = ""
        if self.action_time == "immediate":
            time = ""
        else:
            try:
                time = parser.parse(self.action_time)
            except Exception as e:
                logger.warning(f"Failed to parse time for action \
                               {self.action_name}: {e}")

        report = {
            "name": self.action_name,
            "type": self.action_type,
            "time": time,
            "data": data,
            "metadata": {},
        }

        for key, value in resolved_config.items():
            report["metadata"][key] = value

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
        logger.info(
            f"Executing action: {self.action_parser.action_name} "
            f"of type: {self.action_parser.action_type}"
        )
        data = {}
        for key in self.keys:
            data[key] = self.intermediate_storage.load(key)
        logger.info(
            f"Data loaded for action {self.action_parser.action_name}: {data}"
        )
        report = self.action_parser.producer_function(data)
        logger.info(
            f"Action {self.action_parser.action_name} "
            f"produced report: {report}"
        )
        self.writer.write(str(report))
        logger.info(
            f"Report for action {self.action_parser.action_name} "
            f"sent to Kafka topic: {self.writer.topic}"
        )
