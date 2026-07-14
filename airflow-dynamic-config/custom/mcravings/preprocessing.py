import logging
import os
import sys

_SRC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "src")
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

import pandas as pd
from airflow.exceptions import AirflowSkipException

from mcraving.realtime.avro import records_bounds
from mcraving.realtime.pipeline import avro_records_to_feature_matrix

logger = logging.getLogger(__name__)

# Dotted path to the Empatica participant inside a decoded AVRO record. The
# participant is nested under ``enrollment``; there is no top-level field.
_PARTICIPANT_PATH = ("enrollment", "participantID")


def _participant_id(record):
    """Resolve ``enrollment.participantID`` in a record, or None if missing."""
    current = record
    for segment in _PARTICIPANT_PATH:
        if not isinstance(current, dict):
            return None
        current = current.get(segment)
    return current


def _record_start(record):
    """Earliest raw sensor time in a record; epoch when it carries none so
    such records never sort ahead of timestamped ones as the latest message."""
    try:
        start, _ = records_bounds([record])
    except ValueError:
        return pd.Timestamp(0, tz="UTC")
    return start


class PreprocessingTaskProcessor():
    def __init__(self):
        super().__init__()

    def execute(self, data):
        logger.info(f"Preprocessing data size: {len(data)}")

        # Flatten every upstream/topic into a single record list.
        records = []
        for value in data.values():
            for datum in value.values():
                records += datum
        logger.info(f"Collected {len(records)} avro records")

        # Nothing arrived this run: skip so downstream prediction/notification
        # are skipped too rather than failing on empty input.
        if not records:
            raise AirflowSkipException("No avro records to preprocess.")

        # Group by Empatica participant so each participant's feature rows are
        # built only from that participant's records.
        by_participant = {}
        for record in records:
            participant_id = _participant_id(record)
            if participant_id is None:
                logger.warning("Skipping record without enrollment.participantID")
                continue
            by_participant.setdefault(participant_id, []).append(record)

        frames = []
        for participant_id, participant_records in by_participant.items():
            # Order by sensor time: the newest record is the "latest message"
            # (the interval to score) and the rest are the past messages that
            # provide history.
            ordered = sorted(participant_records, key=_record_start)
            latest_records = [ordered[-1]]
            logger.info(
                f"Participant {participant_id}: {len(ordered)} records "
                f"(1 latest, {len(ordered) - 1} past)"
            )
            try:
                features = avro_records_to_feature_matrix(
                    ordered,
                    latest_records=latest_records,
                    participant_id=participant_id,
                )
            except ValueError as exc:
                # Not enough usable signal for this participant (e.g. no sensor
                # samples in the window): drop them rather than fail the task.
                logger.warning(
                    f"Participant {participant_id}: skipping, insufficient data "
                    f"({exc})"
                )
                continue
            if features.empty:
                logger.warning(
                    f"Participant {participant_id}: skipping, no feature rows"
                )
                continue
            frames.append(features)

        # No participant produced usable features: skip the run gracefully.
        if not frames:
            raise AirflowSkipException(
                "No participant had enough data to build feature rows."
            )

        preprocessed_data = pd.concat(frames, ignore_index=True)
        return preprocessed_data.to_dict(orient='records')
