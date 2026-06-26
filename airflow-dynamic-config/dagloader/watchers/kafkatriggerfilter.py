"""Apply-function used by ``KafkaMessageQueueTrigger`` to decide which Kafka
messages should wake a watcher.

The trigger runs in the *triggerer* process and imports ``match_by_keys`` by
its dotted path (``dagloader.watchers.kafkatriggerfilter.match_by_keys``),
calling it against each ``confluent_kafka.Message`` one at a time. Returning a
truthy value fires a ``TriggerEvent`` carrying that value; returning a falsy
value tells the trigger to keep waiting.

"Filtering by keys" here means: decode the message payload and require every
configured field (``keys``) to be present. The fields are resolved as dotted
paths so nested RADAR records (e.g. ``enrollment.participantID``) work. When a
message matches, the extracted ``field -> value`` mapping is returned so the
event payload identifies what was matched.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Cache the Avro deserializer per schema-registry URL: the trigger calls this
# function once per message and rebuilding the registry client each time would
# be wasteful.
_AVRO_DESERIALIZERS: Dict[str, Any] = {}


def _get_avro_deserializer(schema_registry_url: str):
    deserializer = _AVRO_DESERIALIZERS.get(schema_registry_url)
    if deserializer is None:
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroDeserializer

        client = SchemaRegistryClient({"url": schema_registry_url})
        deserializer = AvroDeserializer(client)
        _AVRO_DESERIALIZERS[schema_registry_url] = deserializer
    return deserializer


def _decode_value(message, format: str, schema_registry_url: Optional[str]):
    """Decode a Kafka message value to a dict, or return None if undecodable."""
    value = message.value()
    if value is None:
        return None
    if format == "json":
        if isinstance(value, (bytes, bytearray)):
            value = value.decode("utf-8")
        return json.loads(value)
    if format == "avro":
        if not schema_registry_url:
            raise ValueError(
                "schema_registry_url is required when format is 'avro'"
            )
        from confluent_kafka.serialization import (
            MessageField,
            SerializationContext,
        )

        deserializer = _get_avro_deserializer(schema_registry_url)
        return deserializer(
            value, SerializationContext(message.topic(), MessageField.VALUE)
        )
    logger.warning("Unsupported Kafka message format '%s'", format)
    return None


def _resolve_field(record: Any, dotted_path: str):
    """Walk a dotted path (e.g. ``enrollment.participantID``) into a mapping.

    Returns the resolved value, or None if any segment is missing."""
    current = record
    for segment in dotted_path.split("."):
        if not isinstance(current, dict) or segment not in current:
            return None
        current = current[segment]
    return current


def _as_list(keys) -> List[str]:
    if keys is None:
        return []
    if isinstance(keys, list):
        return [str(k) for k in keys]
    return [str(keys)]


def match_by_keys(
    message,
    keys=None,
    format: str = "json",
    schema_registry_url: Optional[str] = None,
):
    """Return a truthy ``field -> value`` mapping when the message matches.

    A message matches when its decoded payload contains every field named in
    ``keys``. With no ``keys`` configured, any decodable message matches and the
    whole decoded payload is returned.
    """
    try:
        decoded = _decode_value(message, format, schema_registry_url)
    except Exception as exc:  # noqa: BLE001 - never let a bad record kill the triggerer
        logger.warning("Failed to decode Kafka message: %s", exc)
        return None

    if decoded is None:
        return None

    key_fields = _as_list(keys)
    if not key_fields:
        return decoded

    matched: Dict[str, Any] = {}
    for field in key_fields:
        value = _resolve_field(decoded, field)
        if value is None:
            # A required key field is absent: not a match, keep waiting.
            return None
        matched[field] = value
    return matched
