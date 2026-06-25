"""Read and align Empatica AVRO records on uniform time grids."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

import numpy as np
import pandas as pd
from fastavro import reader


NOMINAL_SAMPLE_RATES = {
    "accelerometer": 64.0,
    "eda": 4.0,
    "temperature": 1.0,
}


def read_avro_records(path: str | Path) -> list[dict]:
    """Read all records from one Empatica AVRO file."""
    with Path(path).open("rb") as stream:
        return list(reader(stream))


def read_avro_files(paths: Iterable[str | Path]) -> list[dict]:
    """Read and flatten records from multiple Empatica AVRO files."""
    return [
        record
        for path in paths
        for record in read_avro_records(path)
    ]


def _stream_bounds(record: dict, stream_name: str) -> tuple[pd.Timestamp, pd.Timestamp] | None:
    stream = record.get("rawData", {}).get(stream_name, {})
    values = stream.get("values", stream.get("x", []))
    rate = float(stream.get("samplingFrequency") or 0)
    timestamp = stream.get("timestampStart")
    if not values or rate <= 0 or not timestamp:
        return None
    start = pd.to_datetime(timestamp, unit="us", utc=True)
    end = start + pd.Timedelta(seconds=len(values) / rate)
    return start, end


def records_bounds(records: Iterable[dict]) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Return the earliest and latest raw sensor times in a record sequence."""
    bounds = [
        bound
        for record in records
        for name in NOMINAL_SAMPLE_RATES
        if (bound := _stream_bounds(record, name)) is not None
    ]
    if not bounds:
        raise ValueError("The AVRO records contain no supported sensor samples.")
    return min(start for start, _ in bounds), max(end for _, end in bounds)


def _empty_grid(
    start: pd.Timestamp,
    end: pd.Timestamp,
    rate: float,
    columns: int,
) -> np.ndarray:
    sample_count = max(0, int(np.ceil((end - start).total_seconds() * rate)))
    return np.full((sample_count, columns), np.nan, dtype=float)


def _write_uniform_samples(
    target: np.ndarray,
    values: np.ndarray,
    timestamp_start,
    source_rate: float,
    target_start: pd.Timestamp,
    target_rate: float,
) -> None:
    """Place samples by timestamp, allowing gaps and overlapping AVRO files."""
    if values.size == 0 or source_rate <= 0:
        return
    stream_start = pd.to_datetime(timestamp_start, unit="us", utc=True)
    first_position = (stream_start - target_start).total_seconds() * target_rate
    positions = np.rint(
        first_position
        + np.arange(len(values), dtype=float) * target_rate / source_rate
    ).astype(np.int64)
    valid = (positions >= 0) & (positions < len(target))
    if not valid.any():
        return
    target[positions[valid]] = values[valid]


def _accelerometer_values(stream: dict) -> np.ndarray:
    values = np.column_stack([stream["x"], stream["y"], stream["z"]]).astype(float)
    params = stream.get("imuParams", {})
    factor = params.get("conversionFactor")
    if not factor:
        digital_max = params.get("digitalMax")
        physical_max = params.get("physicalMax")
        factor = physical_max / digital_max if digital_max else 1.0
    return values * float(factor)


def align_raw_signals(
    records: Iterable[dict],
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """
    Align accelerometer, EDA, temperature, and systolic peaks.

    Systolic peak timestamps are returned as a sorted nanosecond datetime array.
    """
    start = pd.to_datetime(start, utc=True)
    end = pd.to_datetime(end, utc=True)
    accelerometer = _empty_grid(start, end, 64.0, 3)
    eda = _empty_grid(start, end, 4.0, 1)
    temperature = _empty_grid(start, end, 1.0, 1)
    peak_times = []

    for record in records:
        raw = record.get("rawData", {})
        acc_stream = raw.get("accelerometer", {})
        if acc_stream.get("x"):
            _write_uniform_samples(
                accelerometer,
                _accelerometer_values(acc_stream),
                acc_stream["timestampStart"],
                float(acc_stream["samplingFrequency"]),
                start,
                64.0,
            )

        for name, target, rate in (
            ("eda", eda, 4.0),
            ("temperature", temperature, 1.0),
        ):
            stream = raw.get(name, {})
            if stream.get("values"):
                _write_uniform_samples(
                    target,
                    np.asarray(stream["values"], dtype=float)[:, None],
                    stream["timestampStart"],
                    float(stream["samplingFrequency"]),
                    start,
                    rate,
                )

        peaks = raw.get("systolicPeaks", {}).get("peaksTimeNanos", [])
        if peaks:
            peak_times.extend(peaks)

    peaks = np.unique(np.asarray(peak_times, dtype=np.int64))
    if peaks.size:
        lower = start.value
        upper = end.value
        peaks = peaks[(peaks >= lower) & (peaks < upper)]
    return accelerometer, eda, temperature, peaks


def _validate_single_device(records: Iterable[dict]) -> str | None:
    device_serials = {
        record.get("deviceSn")
        for record in records
        if record.get("deviceSn")
    }
    if len(device_serials) > 1:
        raise ValueError(
            "AVRO records contain multiple devices: "
            f"{sorted(device_serials)}"
        )
    return next(iter(device_serials), None)


def load_avro_record_context(
    records: Iterable[dict],
    latest_records: Iterable[dict] | None = None,
    *,
    history_minutes: int = 60,
) -> dict:
    """Align in-memory AVRO records and identify the newest-record interval."""
    records = list(records)
    if not records:
        raise ValueError("records must contain at least one AVRO record.")
    latest_records = list(latest_records) if latest_records is not None else [records[-1]]
    if not latest_records:
        raise ValueError("latest_records must contain at least one AVRO record.")

    device_serial = _validate_single_device(records)
    latest_device_serial = _validate_single_device(latest_records)
    if device_serial and latest_device_serial and device_serial != latest_device_serial:
        raise ValueError(
            "latest_records belong to a different device: "
            f"{latest_device_serial!r} vs {device_serial!r}"
        )

    latest_start, latest_end = records_bounds(latest_records)
    context_start = latest_start - pd.Timedelta(minutes=history_minutes)
    accelerometer, eda, temperature, peaks = align_raw_signals(
        records, context_start, latest_end
    )
    return {
        "accelerometer": accelerometer,
        "eda": eda,
        "temperature": temperature,
        "systolic_peaks_ns": peaks,
        "context_start": context_start,
        "latest_start": latest_start,
        "latest_end": latest_end,
        "latest_file": None,
        "input_files": None,
        "device_serial": device_serial or latest_device_serial,
    }


def load_avro_context(
    avro_files: Iterable[str | Path],
    latest_avro_file: str | Path | None = None,
    *,
    history_minutes: int = 60,
) -> dict:
    """Read files and return aligned raw context plus the newest-file interval."""
    paths = [Path(path) for path in avro_files]
    if not paths:
        raise ValueError("avro_files must contain at least one file.")
    latest_path = Path(latest_avro_file) if latest_avro_file else paths[-1]
    if latest_path not in paths:
        paths.append(latest_path)

    records_by_path = {
        path: read_avro_records(path)
        for path in dict.fromkeys(paths)
    }
    all_records = [
        record
        for records in records_by_path.values()
        for record in records
    ]
    context = load_avro_record_context(
        all_records,
        latest_records=records_by_path[latest_path],
        history_minutes=history_minutes,
    )
    context["latest_file"] = latest_path
    context["input_files"] = list(records_by_path)
    return context
