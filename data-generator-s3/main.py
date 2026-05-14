import argparse
import io
import logging
import random
import signal
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from fastavro import parse_schema, writer
import yaml

AVRO_SCHEMA = {
    "name": "radar_record",
    "type": "record",
    "fields": [
        {"name": "participant_id", "type": "string"},
        {"name": "data_type", "type": "string"},
        {"name": "timestamp", "type": "long"},
        {"name": "payload", "type": {"type": "map", "values": "string"}, "default": {}},
    ],
}


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


def resolve_s3_config(config: Dict[str, Any]) -> Dict[str, Any]:
    base_config = dict(config.get("s3", {}) or {})
    environment_name = config.get("environment")
    environments = config.get("environments", {}) or {}
    if environment_name and environment_name in environments:
        env_s3 = environments[environment_name].get("s3", {}) or {}
        base_config.update(env_s3)
    return base_config


def create_s3_client(s3_config: Dict[str, Any]):
    config = Config(s3={"addressing_style": "path"})
    return boto3.client(
        "s3",
        region_name=s3_config.get("region"),
        endpoint_url=s3_config.get("endpoint_url"),
        aws_access_key_id=s3_config.get("access_key"),
        aws_secret_access_key=s3_config.get("secret_key"),
        config=config,
    )


def ensure_bucket(client, bucket: str) -> None:
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError:
        client.create_bucket(Bucket=bucket)


def format_participant_ids(generator: Dict[str, Any]) -> List[str]:
    num_participants = int(generator.get("num_participants", 1))
    width = int(generator.get("participant_id_width", 4))
    return [f"{index:0{width}d}" for index in range(1, num_participants + 1)]


def build_payload(data_type: str) -> Dict[str, str]:
    if data_type == "empatica":
        metric = random.choice(["eda", "bvp", "accel", "temp"])
        value = random.uniform(0.05, 4.5)
        unit = random.choice(["uS", "ms", "g", "c"])
        return {
            "metric": metric,
            "value": f"{value:.4f}",
            "unit": unit,
        }
    return {
        "value": f"{random.random():.4f}",
    }


def build_object_key(prefix: str, data_type: str, participant_id: str, timestamp: datetime) -> str:
    cleaned_prefix = prefix.strip("/")
    date_part = timestamp.strftime("%Y-%m-%d")
    time_part = timestamp.strftime("%Y%m%dT%H%M%S%fZ")
    path_parts = [part for part in [cleaned_prefix, data_type,
                                    f"{participant_id}",
                                    f"{date_part}"] if part]
    path = "/".join(path_parts)
    return f"{path}/{time_part}.avro"


def generate_avro_bytes(record: Dict[str, Any], schema) -> bytes:
    buffer = io.BytesIO()
    writer(buffer, schema, [record])
    return buffer.getvalue()


def run_generator(
    generator_config: Dict[str, Any],
    schema,
    client,
    bucket: str,
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
    interval = float(generator_config.get("interval", 60))
    records_per_interval = int(generator_config.get("records_per_interval", 1))
    data_type = generator_config.get("data_type", "unknown")
    prefix = generator_config.get("path", "")

    logger.info(
        "Starting generator: data_type=%s interval=%s records_per_interval=%s participants=%s",
        data_type,
        interval,
        records_per_interval,
        len(participants),
    )

    while not stop_event.is_set():
        for _ in range(records_per_interval):
            participant_id = random.choice(participants)
            now = datetime.now(timezone.utc)
            record = {
                "participant_id": participant_id,
                "data_type": data_type,
                "timestamp": int(now.timestamp() * 1000),
                "payload": build_payload(data_type),
            }
            object_key = build_object_key(prefix, data_type, participant_id, now)
            payload_bytes = generate_avro_bytes(record, schema)
            client.put_object(
                Bucket=bucket,
                Key=object_key,
                Body=payload_bytes,
                ContentType="application/avro-binary",
            )
            logger.info("Uploaded %s", object_key)

        stop_event.wait(interval)


def main() -> None:
    parser = argparse.ArgumentParser(description="S3 Avro data generator")
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()

    config = load_config(args.config)
    setup_logging(config)

    logger = logging.getLogger("data_generator")
    s3_config = resolve_s3_config(config)
    bucket = s3_config.get("bucket")
    if not bucket:
        raise ValueError("S3 bucket is required in config")

    client = create_s3_client(s3_config)
    ensure_bucket(client, bucket)

    schema = parse_schema(AVRO_SCHEMA)
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
            args=(generator_config, schema, client, bucket, stop_event),
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


if __name__ == "__main__":
    main()
