from airflow.sdk import Asset, AssetWatcher
from airflow.providers.apache.kafka.triggers.msg_queue import (
    KafkaMessageQueueTrigger,
)
import logging

logger = logging.getLogger(__name__)

# Dotted path the triggerer imports to filter messages. Must stay importable
# from the dags_folder (which Airflow puts on sys.path in every component).
APPLY_FUNCTION = "dagloader.watchers.kafkatriggerfilter.match_by_keys"


class KafkaWatcher:
    """Event-driven Kafka watcher exposed as an Airflow ``Asset``.

    The asset is monitored by a ``KafkaMessageQueueTrigger`` that runs
    continuously in the triggerer. Every message whose decoded payload contains
    all configured ``keys`` fields emits an asset event whose payload is the
    matched ``field -> value`` mapping (e.g. ``{"participant_id": "p123"}``).
    Any DAG scheduled on this asset starts a new run per event, and the run can
    read the matched payload from ``triggering_asset_events`` to process only
    that participant.
    """

    def __init__(self, name: str, config: dict):
        self.name = name
        cfg = config or {}
        self.conn_id = cfg.get('conn_id', 'kafka_default')
        topics = cfg.get('topics') or cfg.get('topic')
        if isinstance(topics, list):
            self.topics = topics
        elif topics:
            self.topics = [topics]
        else:
            self.topics = []
        keys = cfg.get('keys')
        if isinstance(keys, list):
            self.keys = keys
        elif keys:
            self.keys = [keys]
        else:
            self.keys = []
        self.format = cfg.get('format', 'json')
        self.schema_registry_url = cfg.get('schema_registry_url')
        if self.format == 'avro' and not self.schema_registry_url:
            raise ValueError(
                "schema_registry_url is required when format is 'avro'"
            )
        # poll_timeout: how long each Kafka poll waits; poll_interval: how long
        # the trigger sleeps after reaching the end of the log.
        self.poll_timeout = float(cfg.get('poll_timeout', 1))
        self.poll_interval = float(
            cfg.get('poll_interval', cfg.get('poke_interval', 5))
        )
        if not self.topics:
            raise ValueError(
                f"Kafka watcher '{name}' has no topics configured."
            )

    def build_trigger(self) -> KafkaMessageQueueTrigger:
        return KafkaMessageQueueTrigger(
            topics=self.topics,
            kafka_config_id=self.conn_id,
            apply_function=APPLY_FUNCTION,
            apply_function_kwargs={
                "keys": self.keys,
                "format": self.format,
                "schema_registry_url": self.schema_registry_url,
            },
            poll_timeout=self.poll_timeout,
            poll_interval=self.poll_interval,
        )

    def build_asset(self, asset_name: str) -> Asset:
        logger.info(
            "Building Kafka watcher asset '%s' topics=%s keys=%s",
            asset_name, self.topics, self.keys,
        )
        return Asset(
            name=asset_name,
            watchers=[AssetWatcher(name=self.name, trigger=self.build_trigger())],
        )
