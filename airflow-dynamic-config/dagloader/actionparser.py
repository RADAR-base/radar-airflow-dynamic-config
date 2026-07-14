from airflow.providers.apache.kafka.hooks.produce import KafkaProducerHook
from airflow.sdk import BaseOperator
import copy
import json
import logging
import re
from typing import Any
from dateutil import parser

from dagloader.backoff import BackoffFilter

logger = logging.getLogger(__name__)


class KafkaWriter():
    def __init__(self, topic: str, kafka_conn_id: str):
        self.topic = topic
        self.kafka_conn_id = kafka_conn_id

    def write(self, data):
        producer_hook = KafkaProducerHook(kafka_config_id=self.kafka_conn_id)
        producer = producer_hook.get_producer()
        if isinstance(data, (dict, list)):
            value = json.dumps(data, default=str).encode('utf-8')
        elif isinstance(data, str):
            value = data.encode('utf-8')
        else:
            value = data
        try:
            producer.produce(
                topic=self.topic,
                value=value
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
                time = parser.parse(self.action_time).isoformat()
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
    """Action operator driven by a unified-format action entry.

    Same pattern as the sensors / tasks / data sources:
      - `_initial_action_config` holds the pristine YAML entry.
      - `action_config` is the active version mutated by `pre_execute`.
      - `_resolve_config()` rebuilds the `ActionParser`, the storage key,
        and the Kafka writer from `action_config`.
    ParamMaker emits one Param per action keyed by name (= `task_id`)
    whose default is the action's `config` block; that override merges
    into `action_config['config']`.
    """

    def __init__(self, action_config: dict, intermediate_storage,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.intermediate_storage = intermediate_storage
        self._initial_action_config = copy.deepcopy(action_config) or {}
        self.action_config = copy.deepcopy(self._initial_action_config)
        self.kafka_conn_id = self.action_config.get('config',
                                                    {}).get('kafka_conn_id',
                                                            'kafka_default')
        self.kafka_topic = self.action_config.get('config',
                                                  {}).get('topic',
                                                          'output_topic')
        self._resolve_config()

    def _resolve_config(self):
        self.action_parser = ActionParser(self.action_config,
                                          output_topic=self.kafka_topic,
                                          kafka_conn_id=self.kafka_conn_id)
        self.key = f"{self.action_config.get('name', '')}_condition"
        self.writer = KafkaWriter(
            topic=self.action_parser.OUTPUT_TOPIC,
            kafka_conn_id=self.action_parser.KAFKA_CONN_ID,
        )
        self.backoff_filter = BackoffFilter(
            action_name=self.action_config.get('name', ''),
            backoff_config=(self.action_config.get('config') or {}).get('backoff'),
            intermediate_storage=self.intermediate_storage,
        )

    def pre_execute(self, context):
        super().pre_execute(context)
        params = (context or {}).get('params') or {}
        override = params.get(self.task_id)
        if not isinstance(override, dict) or not override:
            return
        new_action_config = copy.deepcopy(self._initial_action_config)
        base_config = new_action_config.get('config') or {}
        new_action_config['config'] = {**base_config, **override}
        if new_action_config == self.action_config:
            return
        logger.info(
            f"Action '{self.task_id}' applying runtime config override: "
            f"{sorted(override.keys())}"
        )
        self.action_config = new_action_config
        self.kafka_conn_id = self.action_config.get('config', {}).get('kafka_conn_id',
                                                                'kafka_default')
        self.kafka_topic = self.action_config.get('config', {}).get('topic',
                                                            'output_topic')
        self._resolve_config()

    def execute(self, context):
        logger.info(
            f"Executing action: {self.action_parser.action_name} "
            f"of type: {self.action_parser.action_type}"
        )
        data = {}
        data[self.key] = self.intermediate_storage.load(self.key)
        logger.info(
            f"Data loaded for action {self.action_parser.action_name}: {data}"
        )
        if data[self.key] is None:
            logger.warning(
                f"No data found for key '{self.key}' in action "
                f"{self.action_parser.action_name}. Action will be skipped."
            )

        elif isinstance(data[self.key], list):
            filtered = self.backoff_filter.apply(data[self.key])
            logger.info("After backoff filter, %d items remain for action "
                        "%s.", len(filtered), self.action_parser.action_name)
            if not filtered:
                logger.info(
                    f"Action {self.action_parser.action_name} skipped: "
                    "all items filtered by backoff."
                )
                return
            for datum in filtered:
                single_data = datum
                report = self.action_parser.producer_function(single_data)
                logger.info(
                    f"Action {self.action_parser.action_name} "
                    f"produced report: {report}"
                )
                self.writer.write(report)
                logger.info(
                    f"Report for action {self.action_parser.action_name} "
                    f"sent to Kafka topic: {self.writer.topic}"
                )
        else:
            report = self.action_parser.producer_function(data)
            logger.info(
                f"Action {self.action_parser.action_name} "
                f"produced report: {report}"
            )
            self.writer.write(report)
            logger.info(
                f"Report for action {self.action_parser.action_name} "
                f"sent to Kafka topic: {self.writer.topic}"
            )
