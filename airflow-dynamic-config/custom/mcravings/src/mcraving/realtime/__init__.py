"""Real-time AVRO preprocessing and feature extraction."""

from mcraving.realtime.avro import read_avro_files, read_avro_records
from mcraving.realtime.pipeline import (
    RealtimeSignals,
    avro_files_to_feature_matrix,
    avro_records_to_feature_matrix,
    preprocess_avro_files,
    preprocess_avro_records,
)

__all__ = [
    "RealtimeSignals",
    "avro_files_to_feature_matrix",
    "avro_records_to_feature_matrix",
    "preprocess_avro_files",
    "preprocess_avro_records",
    "read_avro_files",
    "read_avro_records",
]
