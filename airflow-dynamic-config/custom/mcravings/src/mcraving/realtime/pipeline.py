"""AVRO-to-feature pipeline for near-real-time model inference."""

from __future__ import annotations

from dataclasses import dataclass
import numpy as np
import pandas as pd

from mcraving.features.windowed import (
    append_sociodemographics,
    extract_sliding_window_features,
)
from mcraving.preprocess.acc import (
    gravity_filter,
    linear_filter,
    magnitude,
    pitch,
    roll,
)
from mcraving.preprocess.eda import sparsEDA
from mcraving.realtime.avro import load_avro_context, load_avro_record_context


@dataclass
class RealtimeSignals:
    """Preprocessed signals aligned to one shared context interval."""

    recording_start: pd.Timestamp
    latest_start: pd.Timestamp
    latest_end: pd.Timestamp
    accelerometer: np.ndarray
    eda: np.ndarray
    temperature: np.ndarray
    heart_rate: np.ndarray
    device_serial: str | None


def _finite_runs(values: np.ndarray) -> list[slice]:
    mask = np.isfinite(values)
    return _true_runs(mask)


def _true_runs(mask: np.ndarray) -> list[slice]:
    mask = np.asarray(mask, dtype=bool)
    edges = np.diff(mask.astype(np.int8))
    starts = list(np.flatnonzero(edges == 1) + 1)
    ends = list(np.flatnonzero(edges == -1) + 1)
    if mask.size and mask[0]:
        starts.insert(0, 0)
    if mask.size and mask[-1]:
        ends.append(len(mask))
    return [slice(start, end) for start, end in zip(starts, ends)]


def _preprocess_accelerometer(raw: np.ndarray) -> np.ndarray:
    output = np.full((len(raw), 9), np.nan, dtype=float)
    for run in _true_runs(np.isfinite(raw).all(axis=1)):
        if run.stop - run.start < 320:
            continue
        values = raw[run]
        linear = linear_filter(values, 64.0)
        gravity = gravity_filter(values, 64.0)
        output[run, 0:3] = linear
        output[run, 3:6] = gravity
        output[run, 6] = magnitude(values[:, 0], values[:, 1], values[:, 2])
        output[run, 7] = roll(gravity[:, 1], gravity[:, 2])
        output[run, 8] = pitch(gravity[:, 0], gravity[:, 1], gravity[:, 2])
    return output


def _preprocess_eda(raw: np.ndarray) -> np.ndarray:
    output = np.full((len(raw), 3), np.nan, dtype=float)
    values = raw[:, 0]
    for run in _finite_runs(values):
        if run.stop - run.start <= 320:
            continue
        driver, scl, mse = sparsEDA(
            values[run],
            4,
            epsilon=1,
            Kmax=40,
            dmin=1.25 * 4,
            rho=0.025,
        )
        output[run] = np.column_stack([driver, scl, mse])
    return output


def _aggregate_minutes(
    values: np.ndarray,
    samples_per_minute: int,
) -> np.ndarray:
    minute_count = len(values) // samples_per_minute
    output = np.full((minute_count, values.shape[1]), np.nan, dtype=float)
    for minute in range(minute_count):
        segment = values[
            minute * samples_per_minute : (minute + 1) * samples_per_minute
        ]
        if np.isfinite(segment).any():
            output[minute] = np.nanmean(segment, axis=0)
    return output


def _heart_rate_minutes(
    peak_times_ns: np.ndarray,
    start: pd.Timestamp,
    minute_count: int,
) -> np.ndarray:
    output = np.full((minute_count, 1), np.nan, dtype=float)
    if peak_times_ns.size < 2:
        return output
    peak_minutes = (peak_times_ns - start.value) / 60_000_000_000
    rr_seconds = np.diff(peak_times_ns) / 1_000_000_000
    midpoint_minutes = (peak_minutes[:-1] + peak_minutes[1:]) / 2
    plausible = (rr_seconds >= 0.3) & (rr_seconds <= 2.5)
    for minute in range(minute_count):
        selected = plausible & (midpoint_minutes >= minute) & (
            midpoint_minutes < minute + 1
        )
        if selected.any():
            output[minute, 0] = 60.0 / np.median(rr_seconds[selected])
    return output


def preprocess_avro_records(
    records,
    latest_records=None,
    *,
    history_minutes: int = 60,
) -> RealtimeSignals:
    """
    Preprocess in-memory AVRO records.

    EDA decomposition and minute temperature values remain in their original
    scales, matching the ``eda`` and ``temperature`` arrays used offline.

    ``latest_records`` should contain the records read from the newest AVRO
    file. If omitted, the final record in ``records`` is treated as the newest
    record.
    """
    context = load_avro_record_context(
        records,
        latest_records=latest_records,
        history_minutes=history_minutes,
    )
    return _preprocess_context(context)


def preprocess_avro_files(
    avro_files,
    latest_avro_file=None,
    *,
    history_minutes: int = 60,
) -> RealtimeSignals:
    """Read AVRO files from disk, then preprocess their records."""
    context = load_avro_context(
        avro_files,
        latest_avro_file,
        history_minutes=history_minutes,
    )
    return _preprocess_context(context)


def _preprocess_context(context: dict) -> RealtimeSignals:
    """Preprocess an already aligned raw AVRO context."""
    accelerometer = _preprocess_accelerometer(context["accelerometer"])
    eda = _preprocess_eda(context["eda"])

    minute_count = int(
        (context["latest_end"] - context["context_start"]).total_seconds() // 60
    )
    temperature = _aggregate_minutes(context["temperature"], 60)[:minute_count]
    heart_rate = _heart_rate_minutes(
        context["systolic_peaks_ns"],
        context["context_start"],
        minute_count,
    )
    return RealtimeSignals(
        recording_start=context["context_start"],
        latest_start=context["latest_start"],
        latest_end=context["latest_end"],
        accelerometer=accelerometer,
        eda=eda,
        temperature=temperature,
        heart_rate=heart_rate,
        device_serial=context["device_serial"],
    )


def _slice_from_time(
    values: np.ndarray,
    source_start: pd.Timestamp,
    target_start: pd.Timestamp,
    sample_rate: float,
) -> np.ndarray:
    index = max(
        0,
        int(round((target_start - source_start).total_seconds() * sample_rate)),
    )
    return values[index:]


def _signals_to_feature_matrix(
    signals: RealtimeSignals,
    *,
    participant_id: str | int | None = None,
    sociodemographics: pd.DataFrame | None = None,
    window_minutes: int = 40,
    step_minutes: int = 5,
    minimum_usable_fraction: float = 0.8,
    zero_tolerance: float = 0.0,
    drop_insufficient: bool = False,
) -> pd.DataFrame:
    """Convert preprocessed real-time signals into feature rows."""
    if window_minutes <= 0 or step_minutes <= 0:
        raise ValueError("window_minutes and step_minutes must be positive.")
    first_end = signals.latest_start + pd.Timedelta(minutes=step_minutes)
    first_start = first_end - pd.Timedelta(minutes=window_minutes)
    if first_end > signals.latest_end:
        return pd.DataFrame()

    accelerometer = _slice_from_time(
        signals.accelerometer, signals.recording_start, first_start, 64.0
    )
    eda = _slice_from_time(
        signals.eda, signals.recording_start, first_start, 4.0
    )
    temperature = _slice_from_time(
        signals.temperature, signals.recording_start, first_start, 1 / 60
    )
    heart_rate = _slice_from_time(
        signals.heart_rate, signals.recording_start, first_start, 1 / 60
    )
    features = extract_sliding_window_features(
        accelerometer,
        eda,
        temperature,
        heart_rate,
        first_start,
        participant_id=participant_id,
        window_minutes=window_minutes,
        step_minutes=step_minutes,
        minimum_usable_fraction=minimum_usable_fraction,
        zero_tolerance=zero_tolerance,
        drop_incomplete=False,
        drop_insufficient=False,
    )
    features = features.loc[features["window_end"] <= signals.latest_end].copy()
    if participant_id is not None and sociodemographics is not None:
        features = append_sociodemographics(
            features,
            participant_id,
            sociodemographics,
        )
    features["latest_avro_start"] = signals.latest_start
    features["latest_avro_end"] = signals.latest_end
    features["device_serial"] = signals.device_serial
    if drop_insufficient:
        features = features.loc[features["sufficient_data"].fillna(False)]
    return features.reset_index(drop=True)


def avro_records_to_feature_matrix(
    records,
    latest_records=None,
    *,
    participant_id: str | int | None = None,
    sociodemographics: pd.DataFrame | None = None,
    history_minutes: int = 60,
    window_minutes: int = 40,
    step_minutes: int = 5,
    minimum_usable_fraction: float = 0.8,
    zero_tolerance: float = 0.0,
    drop_insufficient: bool = False,
) -> pd.DataFrame:
    """
    Convert in-memory Empatica AVRO records into inference-ready feature rows.

    ``latest_records`` should be the record list from the newest AVRO file. If
    omitted, the final record in ``records`` is treated as the newest record.
    """
    if window_minutes <= 0 or step_minutes <= 0:
        raise ValueError("window_minutes and step_minutes must be positive.")
    required_history = max(0, window_minutes - step_minutes)
    if history_minutes < required_history:
        raise ValueError(
            f"history_minutes must be at least {required_history} to construct "
            "the first requested window."
        )
    signals = preprocess_avro_records(
        records,
        latest_records=latest_records,
        history_minutes=history_minutes,
    )
    return _signals_to_feature_matrix(
        signals,
        participant_id=participant_id,
        sociodemographics=sociodemographics,
        window_minutes=window_minutes,
        step_minutes=step_minutes,
        minimum_usable_fraction=minimum_usable_fraction,
        zero_tolerance=zero_tolerance,
        drop_insufficient=drop_insufficient,
    )


def avro_files_to_feature_matrix(
    avro_files,
    latest_avro_file=None,
    *,
    participant_id: str | int | None = None,
    sociodemographics: pd.DataFrame | None = None,
    history_minutes: int = 60,
    window_minutes: int = 40,
    step_minutes: int = 5,
    minimum_usable_fraction: float = 0.8,
    zero_tolerance: float = 0.0,
    drop_insufficient: bool = False,
) -> pd.DataFrame:
    """
    Read recent Empatica AVRO files and return inference-ready feature rows.

    This is a convenience wrapper around ``avro_records_to_feature_matrix``.
    Windows end 5, 10, 15, ... minutes into the newest AVRO interval.
    """
    if window_minutes <= 0 or step_minutes <= 0:
        raise ValueError("window_minutes and step_minutes must be positive.")
    required_history = max(0, window_minutes - step_minutes)
    if history_minutes < required_history:
        raise ValueError(
            f"history_minutes must be at least {required_history} to construct "
            "the first requested window."
        )
    signals = preprocess_avro_files(
        avro_files,
        latest_avro_file,
        history_minutes=history_minutes,
    )
    return _signals_to_feature_matrix(
        signals,
        participant_id=participant_id,
        sociodemographics=sociodemographics,
        window_minutes=window_minutes,
        step_minutes=step_minutes,
        minimum_usable_fraction=minimum_usable_fraction,
        zero_tolerance=zero_tolerance,
        drop_insufficient=drop_insufficient,
    )
