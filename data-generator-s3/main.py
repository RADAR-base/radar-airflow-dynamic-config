import argparse
import json
import logging
import math
import random
import signal
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import yaml
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

# Avro schema mirroring the Empatica EmbracePlus raw-data export format.
# Named records (Version, ImuStream, FloatStream) are defined once and
# referenced by name where the same shape repeats.
AVRO_SCHEMA = {
    "type": "record",
    "name": "EmpaticaRecord",
    "namespace": "org.radarbase.empatica",
    "fields": [
        {
            "name": "schemaVersion",
            "type": {
                "type": "record",
                "name": "Version",
                "fields": [
                    {"name": "major", "type": "int"},
                    {"name": "minor", "type": "int"},
                    {"name": "patch", "type": "int"},
                ],
            },
        },
        {"name": "fwVersion", "type": "Version"},
        {"name": "hwVersion", "type": "Version"},
        {"name": "algoVersion", "type": "Version"},
        {"name": "timezone", "type": "int"},
        {
            "name": "enrollment",
            "type": {
                "type": "record",
                "name": "Enrollment",
                "fields": [
                    {"name": "participantID", "type": "string"},
                    {"name": "siteID", "type": "string"},
                    {"name": "studyID", "type": "string"},
                    {"name": "organizationID", "type": "string"},
                ],
            },
        },
        {"name": "deviceSn", "type": "string"},
        {"name": "deviceModel", "type": "string"},
        {
            "name": "rawData",
            "type": {
                "type": "record",
                "name": "RawData",
                "fields": [
                    {
                        "name": "accelerometer",
                        "type": {
                            "type": "record",
                            "name": "ImuStream",
                            "fields": [
                                {"name": "timestampStart", "type": "long"},
                                {"name": "samplingFrequency", "type": "double"},
                                {
                                    "name": "imuParams",
                                    "type": {
                                        "type": "record",
                                        "name": "ImuParams",
                                        "fields": [
                                            {"name": "physicalMin", "type": "int"},
                                            {"name": "physicalMax", "type": "int"},
                                            {"name": "digitalMin", "type": "int"},
                                            {"name": "digitalMax", "type": "int"},
                                        ],
                                    },
                                },
                                {"name": "x", "type": {"type": "array", "items": "int"}},
                                {"name": "y", "type": {"type": "array", "items": "int"}},
                                {"name": "z", "type": {"type": "array", "items": "int"}},
                            ],
                        },
                    },
                    {"name": "gyroscope", "type": "ImuStream"},
                    {
                        "name": "eda",
                        "type": {
                            "type": "record",
                            "name": "FloatStream",
                            "fields": [
                                {"name": "timestampStart", "type": "long"},
                                {"name": "samplingFrequency", "type": "double"},
                                {"name": "values", "type": {"type": "array", "items": "float"}},
                            ],
                        },
                    },
                    {"name": "temperature", "type": "FloatStream"},
                    {
                        "name": "tags",
                        "type": {
                            "type": "record",
                            "name": "Tags",
                            "fields": [
                                {"name": "tagsTimeMicros", "type": {"type": "array", "items": "long"}},
                            ],
                        },
                    },
                    {"name": "bvp", "type": "FloatStream"},
                    {
                        "name": "systolicPeaks",
                        "type": {
                            "type": "record",
                            "name": "SystolicPeaks",
                            "fields": [
                                {"name": "peaksTimeNanos", "type": {"type": "array", "items": "long"}},
                            ],
                        },
                    },
                    {
                        "name": "steps",
                        "type": {
                            "type": "record",
                            "name": "StepStream",
                            "fields": [
                                {"name": "timestampStart", "type": "long"},
                                {"name": "samplingFrequency", "type": "double"},
                                {"name": "values", "type": {"type": "array", "items": "int"}},
                            ],
                        },
                    },
                ],
            },
        },
    ],
}

# Nominal sampling rates of each Empatica EmbracePlus stream, in Hz.
ACCEL_HZ = 63.999622
EDA_HZ = 3.9988773
TEMPERATURE_HZ = 0.9997099
BVP_HZ = 64.0
STEPS_HZ = 0.21359096


def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r") as handle:
        return yaml.safe_load(handle) or {}


def setup_logging(config: Dict[str, Any]) -> None:
    logging_config = config.get("logging", {})
    level = logging_config.get("level", "INFO").upper()
    log_format = logging_config.get(
        "format",
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logging.basicConfig(level=level, format=log_format)


def resolve_kafka_config(config: Dict[str, Any]) -> Dict[str, Any]:
    base_config = dict(config.get("kafka", {}) or {})
    environment_name = config.get("environment")
    environments = config.get("environments", {}) or {}
    if environment_name and environment_name in environments:
        env_kafka = environments[environment_name].get("kafka", {}) or {}
        base_config.update(env_kafka)
    return base_config


def delivery_report(err, msg) -> None:
    if err is not None:
        logging.getLogger("data_generator.delivery").error(
            "Delivery failed for key %s: %s", msg.key(), err
        )


def create_producer(kafka_config: Dict[str, Any], schema_str: str) -> SerializingProducer:
    schema_registry_client = SchemaRegistryClient({"url": kafka_config["schema_registry_url"]})
    avro_serializer = AvroSerializer(schema_registry_client, schema_str)
    # A single 30-minute Empatica record serialises to ~1.2 MB, above
    # librdkafka's 1 MB default, so raise the producer ceiling. Must stay in
    # sync with the broker/topic/consumer limits (see docker-compose.yaml).
    message_max_bytes = int(kafka_config.get("message_max_bytes", 10_485_760))
    return SerializingProducer(
        {
            "bootstrap.servers": kafka_config["bootstrap_servers"],
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": avro_serializer,
            "message.max.bytes": message_max_bytes,
        }
    )


def format_participant_ids(generator: Dict[str, Any]) -> List[str]:
    num_participants = int(generator.get("num_participants", 1))
    prefix = generator.get("participant_prefix", "participant")
    width = int(generator.get("participant_id_width", 4))
    return [f"{prefix}-{index:0{width}d}" for index in range(1, num_participants + 1)]


def sample_count(frequency: float, duration_seconds: float) -> int:
    return max(1, int(round(frequency * duration_seconds)))


def build_imu_stream(start_micros: int, duration_seconds: float) -> Dict[str, Any]:
    count = sample_count(ACCEL_HZ, duration_seconds)
    # Centre each axis on the resting baselines seen in real exports and
    # add small gaussian jitter so consecutive samples look plausible.
    axes = {}
    for axis, baseline in (("x", 710), ("y", 1305), ("z", 1448)):
        axes[axis] = [int(round(random.gauss(baseline, 10))) for _ in range(count)]
    return {
        "timestampStart": start_micros,
        "samplingFrequency": ACCEL_HZ,
        "imuParams": {
            "physicalMin": -16,
            "physicalMax": 16,
            "digitalMin": -32768,
            "digitalMax": 32768,
        },
        **axes,
    }


def build_empty_imu_stream() -> Dict[str, Any]:
    # The EmbracePlus export ships gyroscope as an empty, zeroed stream.
    return {
        "timestampStart": 0,
        "samplingFrequency": 0.0,
        "imuParams": {
            "physicalMin": 0,
            "physicalMax": 0,
            "digitalMin": 0,
            "digitalMax": 0,
        },
        "x": [],
        "y": [],
        "z": [],
    }


def build_eda_stream(start_micros: int, duration_seconds: float) -> Dict[str, Any]:
    count = sample_count(EDA_HZ, duration_seconds)
    value = -0.01
    values = []
    for _ in range(count):
        value += random.gauss(0.0, 0.0003)
        values.append(value)
    return {
        "timestampStart": start_micros,
        "samplingFrequency": EDA_HZ,
        "values": values,
    }


def build_temperature_stream(start_micros: int, duration_seconds: float) -> Dict[str, Any]:
    count = sample_count(TEMPERATURE_HZ, duration_seconds)
    # Quantised to the device's ~1/128 C resolution around a resting value.
    values = [round(random.gauss(21.68, 0.02) * 128) / 128 for _ in range(count)]
    return {
        "timestampStart": start_micros,
        "samplingFrequency": TEMPERATURE_HZ,
        "values": values,
    }


def build_bvp_stream(start_micros: int, duration_seconds: float) -> Dict[str, Any]:
    count = sample_count(BVP_HZ, duration_seconds)
    # A ~1 Hz pulse waveform (heart beat) with low-amplitude noise.
    values = []
    for index in range(count):
        phase = 2 * math.pi * index / BVP_HZ
        values.append(0.001 * math.sin(phase) + random.gauss(0.0, 0.00005))
    return {
        "timestampStart": start_micros,
        "samplingFrequency": BVP_HZ,
        "values": values,
    }


def build_systolic_peaks(start_micros: int, duration_seconds: float) -> Dict[str, Any]:
    # One systolic peak roughly every 0.6 s, jittered, expressed in nanos.
    peaks = []
    t = start_micros * 1000
    end = (start_micros + int(duration_seconds * 1_000_000)) * 1000
    while t < end:
        peaks.append(int(t))
        t += int(random.uniform(0.5, 0.7) * 1_000_000_000)
    return {"peaksTimeNanos": peaks}


def build_steps_stream(start_micros: int, duration_seconds: float) -> Dict[str, Any]:
    count = sample_count(STEPS_HZ, duration_seconds)
    # Mostly stationary, with the occasional step count.
    values = [random.choice([0, 0, 0, 0, 1]) for _ in range(count)]
    return {
        "timestampStart": start_micros,
        "samplingFrequency": STEPS_HZ,
        "values": values,
    }


def random_device_serial() -> str:
    return "".join(random.choice("0123456789ABCDEFGHJKLMNPQRSTUVWXYZ") for _ in range(10))


def build_empatica_record(
    participant_id: str,
    generator_config: Dict[str, Any],
    now: datetime,
) -> Dict[str, Any]:
    duration_seconds = float(generator_config.get("duration_seconds", 10))
    enrollment = generator_config.get("enrollment", {}) or {}
    start_micros = int(now.timestamp() * 1_000_000)
    return {
        "schemaVersion": {"major": 6, "minor": 3, "patch": 0},
        "fwVersion": {"major": 3, "minor": 3, "patch": 3},
        "hwVersion": {"major": 6, "minor": 0, "patch": 2},
        "algoVersion": {"major": 6, "minor": 4, "patch": 1},
        "timezone": int(generator_config.get("timezone", 0)),
        "enrollment": {
            "participantID": participant_id,
            "siteID": str(enrollment.get("siteID", "")),
            "studyID": str(enrollment.get("studyID", "")),
            "organizationID": str(enrollment.get("organizationID", "")),
        },
        "deviceSn": "TESTDEVICE",
        "deviceModel": str(generator_config.get("device_model", "EMBRACEPLUS")),
        "rawData": {
            "accelerometer": build_imu_stream(start_micros, duration_seconds),
            "gyroscope": build_empty_imu_stream(),
            "eda": build_eda_stream(start_micros, duration_seconds),
            "temperature": build_temperature_stream(start_micros, duration_seconds),
            "tags": {"tagsTimeMicros": []},
            "bvp": build_bvp_stream(start_micros, duration_seconds),
            "systolicPeaks": build_systolic_peaks(start_micros, duration_seconds),
            "steps": build_steps_stream(start_micros, duration_seconds),
        },
    }


def run_generator(
    generator_config: Dict[str, Any],
    producer: SerializingProducer,
    default_topic: str,
    stop_event: threading.Event,
) -> None:
    logger = logging.getLogger(f"generator.{generator_config.get('data_type', 'unknown')}")
    if not generator_config.get("enabled", True):
        logger.info("Generator disabled; skipping.")
        return

    participants = format_participant_ids(generator_config)
    if not participants:
        logger.warning("No participants configured; skipping generator.")
        return
    interval = float(generator_config.get("interval", 1800))
    backfill_hours = float(generator_config.get("backfill_hours", 2))
    data_type = generator_config.get("data_type", "unknown")
    topic = generator_config.get("topic", default_topic)

    logger.info(
        "Starting generator: data_type=%s topic=%s interval=%s backfill_hours=%s participants=%s",
        data_type,
        topic,
        interval,
        backfill_hours,
        len(participants),
    )

    def emit(participant_id: str, when: datetime) -> None:
        record = build_empatica_record(participant_id, generator_config, when)
        producer.produce(
            topic=topic,
            key=participant_id,
            value=record,
            on_delivery=delivery_report,
        )
        producer.poll(0)
        logger.info("Produced record for %s at %s to %s", participant_id, when.isoformat(), topic)

    interval_delta = timedelta(seconds=interval)
    now = datetime.now(timezone.utc)

    # Backfill: one record per participant for each interval slot across the
    # last `backfill_hours`, oldest first (e.g. 2h / 30min -> 4 slots).
    num_backfill_slots = int(round((backfill_hours * 3600) / interval)) if interval > 0 else 0
    for slot in range(num_backfill_slots, 0, -1):
        slot_time = now - slot * interval_delta
        for participant_id in participants:
            emit(participant_id, slot_time)

    # Ongoing: one record per participant every `interval` seconds.
    while not stop_event.is_set():
        emit_time = datetime.now(timezone.utc)
        for participant_id in participants:
            emit(participant_id, emit_time)

        stop_event.wait(interval)


def main() -> None:
    parser = argparse.ArgumentParser(description="Kafka Empatica Avro data generator")
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()

    config = load_config(args.config)
    setup_logging(config)

    logger = logging.getLogger("data_generator")
    kafka_config = resolve_kafka_config(config)
    if not kafka_config.get("bootstrap_servers"):
        raise ValueError("Kafka bootstrap_servers is required in config")
    if not kafka_config.get("schema_registry_url"):
        raise ValueError("Kafka schema_registry_url is required in config")
    default_topic = kafka_config.get("topic")
    if not default_topic:
        raise ValueError("Kafka topic is required in config")

    producer = create_producer(kafka_config, json.dumps(AVRO_SCHEMA))
    stop_event = threading.Event()

    def handle_shutdown(*_args):
        logger.info("Shutdown signal received. Stopping generators...")
        stop_event.set()

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    generators = config.get("generators", [])
    threads = []
    for generator_config in generators:
        thread = threading.Thread(
            target=run_generator,
            args=(generator_config, producer, default_topic, stop_event),
            daemon=True,
        )
        thread.start()
        threads.append(thread)

    try:
        while not stop_event.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        handle_shutdown()

    for thread in threads:
        thread.join()

    logger.info("Flushing pending messages...")
    producer.flush()


if __name__ == "__main__":
    main()
