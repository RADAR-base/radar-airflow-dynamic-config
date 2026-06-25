"""Sliding-window feature extraction and participant/event annotation."""

from __future__ import annotations

from collections.abc import Mapping

import numpy as np
import pandas as pd
from scipy.stats import iqr, kurtosis, skew

from mcraving.features.generic import (
    gradient,
    hjorth_parameters,
    hurst,
    sampen,
    zero_crossing_count,
)


DEFAULT_SAMPLE_RATES = {
    "accelerometer": 64.0,
    "eda": 4.0,
    "temperature": 1.0 / 60.0,
    "heart_rate": 1.0 / 60.0,
}

STRING_COLUMNS = ("participant_id", "gender", "location")
BOOLEAN_COLUMNS = ("complete_window", "sufficient_data", "crack", "craving_overlap")
UTC_DATETIME_COLUMNS = (
    "window_start",
    "window_end",
    "nearest_craving_timestamp",
)


def _validate_array(values: np.ndarray, column: int, name: str):
    if values.ndim == 1:
        if column != 0:
            raise ValueError(f"{name} is 1D, so column must be 0.")
        return values
    if values.ndim != 2:
        raise ValueError(f"{name} must be a 1D or 2D array.")
    if not 0 <= column < values.shape[1]:
        raise ValueError(
            f"{name} column {column} is outside its {values.shape[1]} columns."
        )
    return values


def _channel_slice(values, column: int, sample_slice: slice) -> np.ndarray:
    if values.ndim == 1:
        return np.asarray(values[sample_slice])
    return np.asarray(values[sample_slice, column])


def _finite(values: np.ndarray) -> np.ndarray:
    return np.asarray(values, dtype=float)[np.isfinite(values)]


def _usable_fraction(values: np.ndarray, zero_tolerance: float) -> float:
    """Fraction of samples that are finite and not effectively zero."""
    values = np.asarray(values, dtype=float)
    if values.size == 0:
        return 0.0
    usable = np.isfinite(values) & (np.abs(values) > zero_tolerance)
    return float(usable.mean())


def _nan_features(prefix: str, names: tuple[str, ...]) -> dict[str, float]:
    return {f"{prefix}{name}": np.nan for name in names}


def accelerometer_features(segment: np.ndarray, prefix: str = "mag_") -> dict:
    """Calculate the accelerometer features used by the original notebook."""
    segment = _finite(segment)
    names = (
        "min",
        "max",
        "std",
        "skew",
        "kurtosis",
        "hjorth_activity",
        "hjorth_mobility",
        "hjorth_complexity",
        "zero_crossings",
    )
    if segment.size < 3:
        return _nan_features(prefix, names)

    hjorth = hjorth_parameters(segment)
    return {
        f"{prefix}min": np.min(segment),
        f"{prefix}max": np.max(segment),
        f"{prefix}std": np.std(segment),
        f"{prefix}skew": skew(segment),
        f"{prefix}kurtosis": kurtosis(segment),
        f"{prefix}hjorth_activity": hjorth[0],
        f"{prefix}hjorth_mobility": hjorth[1],
        f"{prefix}hjorth_complexity": hjorth[2],
        f"{prefix}zero_crossings": zero_crossing_count(segment, th=0.05),
    }


def physiological_features(segment: np.ndarray, prefix: str) -> dict:
    """Calculate the temperature-style features used for temp, HR, and EDA."""
    segment = _finite(segment)
    names = (
        "mean",
        "min",
        "max",
        "std",
        "gradient",
        "range",
        "skew",
        "kurtosis",
        "hjorth_activity",
        "hjorth_mobility",
        "hjorth_complexity",
        "iqr",
        "entropy",
        "hurst",
    )
    if segment.size < 18:
        return _nan_features(prefix, names)

    hjorth = hjorth_parameters(segment)
    entropy = sampen(segment, 3)
    if not np.isfinite(entropy):
        entropy = 0.0

    return {
        f"{prefix}mean": np.mean(segment),
        f"{prefix}min": np.min(segment),
        f"{prefix}max": np.max(segment),
        f"{prefix}std": np.std(segment),
        f"{prefix}gradient": np.mean(gradient(segment)),
        f"{prefix}range": np.max(segment) - np.min(segment),
        f"{prefix}skew": skew(segment),
        f"{prefix}kurtosis": kurtosis(segment),
        f"{prefix}hjorth_activity": hjorth[0],
        f"{prefix}hjorth_mobility": hjorth[1],
        f"{prefix}hjorth_complexity": hjorth[2],
        f"{prefix}iqr": iqr(segment),
        f"{prefix}entropy": entropy,
        f"{prefix}hurst": hurst(segment, [2, 4, 8, 16]),
    }


def _sample_slice(
    start_seconds: float,
    end_seconds: float,
    sample_rate: float,
) -> slice:
    return slice(
        int(round(start_seconds * sample_rate)),
        int(round(end_seconds * sample_rate)),
    )


def _duration_seconds(values: np.ndarray, sample_rate: float) -> float:
    if sample_rate <= 0:
        raise ValueError("Sample rates must be greater than zero.")
    return len(values) / sample_rate


def _empty_feature_frame() -> pd.DataFrame:
    row = {
        "participant_id": None,
        "window_start": pd.NaT,
        "window_end": pd.NaT,
        "window_start_minute": np.nan,
        "window_end_minute": np.nan,
        "complete_window": False,
        "acc_usable_fraction": np.nan,
        "eda_scl_usable_fraction": np.nan,
        "eda_mse_usable_fraction": np.nan,
        "temp_usable_fraction": np.nan,
        "hr_usable_fraction": np.nan,
        "minimum_sensor_usable_fraction": np.nan,
        "sufficient_data": False,
    }
    row.update(
        _nan_features(
            "mag_",
            (
                "min",
                "max",
                "std",
                "skew",
                "kurtosis",
                "hjorth_activity",
                "hjorth_mobility",
                "hjorth_complexity",
                "zero_crossings",
            ),
        )
    )
    physiological_names = (
        "mean",
        "min",
        "max",
        "std",
        "gradient",
        "range",
        "skew",
        "kurtosis",
        "hjorth_activity",
        "hjorth_mobility",
        "hjorth_complexity",
        "iqr",
        "entropy",
        "hurst",
    )
    for prefix in ("temp_", "hr_", "eda_scl_", "eda_mse_"):
        row.update(_nan_features(prefix, physiological_names))
    return pd.DataFrame(columns=row)


def extract_sliding_window_features(
    accelerometer: np.ndarray,
    eda: np.ndarray,
    temperature: np.ndarray,
    heart_rate: np.ndarray,
    recording_start,
    participant_id: str | int | None = None,
    *,
    window_minutes: int = 40,
    step_minutes: int = 5,
    sample_rates: Mapping[str, float] | None = None,
    accelerometer_magnitude_column: int = 6,
    eda_scl_column: int = 1,
    eda_mse_column: int = 2,
    temperature_column: int = 0,
    heart_rate_column: int = 0,
    drop_incomplete: bool = False,
    minimum_usable_fraction: float = 0.8,
    zero_tolerance: float = 0.0,
    drop_insufficient: bool = False,
) -> pd.DataFrame:
    """
    Extract the existing feature set from consecutive sliding windows.

    Each output row describes a half-open interval ``[window_start, window_end)``.
    The common recording duration is limited to the shortest supplied modality.
    By default every possible time window is returned and ``complete_window``
    records data completeness. When ``drop_incomplete`` is true, a window is
    retained only when all channels used by the feature calculations contain
    finite values throughout.

    ``*_usable_fraction`` columns report the proportion of samples that are
    finite and have absolute value greater than ``zero_tolerance``.
    ``sufficient_data`` is true when every used channel meets
    ``minimum_usable_fraction``. Set ``drop_insufficient`` to retain only those
    windows.
    """
    if window_minutes <= 0 or step_minutes <= 0:
        raise ValueError("window_minutes and step_minutes must be positive.")
    if not 0 <= minimum_usable_fraction <= 1:
        raise ValueError("minimum_usable_fraction must lie between zero and one.")
    if zero_tolerance < 0:
        raise ValueError("zero_tolerance must be non-negative.")

    rates = {**DEFAULT_SAMPLE_RATES, **(sample_rates or {})}
    accelerometer = _validate_array(
        accelerometer, accelerometer_magnitude_column, "accelerometer"
    )
    eda = _validate_array(eda, eda_scl_column, "eda")
    _validate_array(eda, eda_mse_column, "eda")
    temperature = _validate_array(
        temperature, temperature_column, "temperature"
    )
    heart_rate = _validate_array(heart_rate, heart_rate_column, "heart_rate")

    duration_seconds = min(
        _duration_seconds(accelerometer, rates["accelerometer"]),
        _duration_seconds(eda, rates["eda"]),
        _duration_seconds(temperature, rates["temperature"]),
        _duration_seconds(heart_rate, rates["heart_rate"]),
    )
    window_seconds = window_minutes * 60
    step_seconds = step_minutes * 60
    if duration_seconds < window_seconds:
        return normalise_feature_dtypes(_empty_feature_frame())

    start_time = pd.to_datetime(recording_start, utc=True)
    rows = []
    for start_seconds in np.arange(
        0, duration_seconds - window_seconds + 1e-9, step_seconds
    ):
        end_seconds = start_seconds + window_seconds
        acc_slice = _sample_slice(
            start_seconds, end_seconds, rates["accelerometer"]
        )
        eda_slice = _sample_slice(start_seconds, end_seconds, rates["eda"])
        temp_slice = _sample_slice(
            start_seconds, end_seconds, rates["temperature"]
        )
        hr_slice = _sample_slice(
            start_seconds, end_seconds, rates["heart_rate"]
        )
        acc_window = _channel_slice(
            accelerometer, accelerometer_magnitude_column, acc_slice
        )
        scl_window = _channel_slice(eda, eda_scl_column, eda_slice)
        mse_window = _channel_slice(eda, eda_mse_column, eda_slice)
        temp_window = _channel_slice(
            temperature, temperature_column, temp_slice
        )
        hr_window = _channel_slice(heart_rate, heart_rate_column, hr_slice)

        complete = all(
            np.isfinite(values).all()
            for values in (
                acc_window,
                scl_window,
                mse_window,
                temp_window,
                hr_window,
            )
        )
        usable_fractions = {
            "acc_usable_fraction": _usable_fraction(
                acc_window, zero_tolerance
            ),
            "eda_scl_usable_fraction": _usable_fraction(
                scl_window, zero_tolerance
            ),
            "eda_mse_usable_fraction": _usable_fraction(
                mse_window, zero_tolerance
            ),
            "temp_usable_fraction": _usable_fraction(
                temp_window, zero_tolerance
            ),
            "hr_usable_fraction": _usable_fraction(
                hr_window, zero_tolerance
            ),
        }
        minimum_sensor_usable_fraction = min(usable_fractions.values())
        sufficient_data = (
            minimum_sensor_usable_fraction >= minimum_usable_fraction
        )
        if drop_incomplete and not complete:
            continue
        if drop_insufficient and not sufficient_data:
            continue

        row = {
            "participant_id": (
                None if participant_id is None else str(participant_id)
            ),
            "window_start": start_time + pd.Timedelta(seconds=start_seconds),
            "window_end": start_time + pd.Timedelta(seconds=end_seconds),
            "window_start_minute": int(start_seconds // 60),
            "window_end_minute": int(end_seconds // 60),
            "complete_window": complete,
            **usable_fractions,
            "minimum_sensor_usable_fraction": (
                minimum_sensor_usable_fraction
            ),
            "sufficient_data": sufficient_data,
        }
        row.update(accelerometer_features(acc_window, prefix="mag_"))
        row.update(physiological_features(temp_window, prefix="temp_"))
        row.update(physiological_features(hr_window, prefix="hr_"))
        row.update(physiological_features(scl_window, prefix="eda_scl_"))
        row.update(physiological_features(mse_window, prefix="eda_mse_"))
        rows.append(row)

    if not rows:
        return normalise_feature_dtypes(_empty_feature_frame())
    return normalise_feature_dtypes(pd.DataFrame(rows))


def normalise_feature_dtypes(features: pd.DataFrame) -> pd.DataFrame:
    """Give every participant shard a stable Arrow-compatible schema."""
    result = features.copy()
    for column in STRING_COLUMNS:
        if column in result:
            result[column] = result[column].astype("string")
    for column in BOOLEAN_COLUMNS:
        if column in result:
            result[column] = result[column].astype("boolean")
    for column in UTC_DATETIME_COLUMNS:
        if column in result:
            result[column] = pd.to_datetime(result[column], utc=True)
    return result


def append_sociodemographics(
    features: pd.DataFrame,
    participant_id: str | int,
    sociodemographics: pd.DataFrame,
    *,
    study_id_column: str = "Study ID",
    participant_id_offset: int = 1000,
) -> pd.DataFrame:
    """Append the notebook's sociodemographic columns to a feature frame."""
    result = features.copy()
    participant_id = str(participant_id)
    study_ids = pd.to_numeric(
        sociodemographics[study_id_column], errors="coerce"
    ).astype("Int64")
    participant_ids = (study_ids + participant_id_offset).astype("string")
    matches = sociodemographics.loc[participant_ids == participant_id]
    if matches.empty:
        raise KeyError(
            f"No sociodemographic row found for participant {participant_id}."
        )

    participant = matches.iloc[0]
    cocaine_type = participant.get("Cocaine type")
    result["participant_id"] = participant_id
    result["gender"] = participant.get("Gender")
    result["age"] = participant.get("Age")
    result["weight"] = participant.get("Weight (Kg)")
    result["height"] = participant.get("Height (cm)")
    result["bmi"] = participant.get("BMI")
    result["location"] = participant.get("Location")
    result["crack"] = (
        isinstance(cocaine_type, str)
        and cocaine_type.strip().lower() == "crack"
    )
    return normalise_feature_dtypes(result)


def append_craving_annotations(
    features: pd.DataFrame,
    participant_id: str | int,
    cravings: pd.DataFrame,
    *,
    participant_column: str = "participant_id",
    timestamp_column: str = "timestamp",
) -> pd.DataFrame:
    """
    Add craving overlap and signed nearest-event timing to every window.

    ``minutes_to_nearest_craving`` is positive when the nearest craving is in
    the future, negative when it is in the past, and zero when a craving falls
    inside the window. Distances are measured from the centre of each window.
    """
    required = {"window_start", "window_end"}
    missing = required.difference(features.columns)
    if missing:
        raise KeyError(f"Feature frame is missing columns: {sorted(missing)}")

    result = features.copy()
    participant_id = str(participant_id)
    participant_cravings = cravings.loc[
        cravings[participant_column].astype(str) == participant_id
    ].copy()
    event_times = (
        pd.to_datetime(participant_cravings[timestamp_column], utc=True)
        .dropna()
        .drop_duplicates()
        .sort_values()
        .reset_index(drop=True)
    )

    result["craving_overlap"] = False
    result["craving_event_count"] = 0
    result["nearest_craving_timestamp"] = pd.Series(
        pd.NaT, index=result.index, dtype="datetime64[ns, UTC]"
    )
    result["minutes_to_nearest_craving"] = np.nan
    if event_times.empty or result.empty:
        return normalise_feature_dtypes(result)

    events = event_times.to_numpy(dtype="datetime64[ns]")
    starts = pd.to_datetime(result["window_start"], utc=True)
    ends = pd.to_datetime(result["window_end"], utc=True)
    centres = starts + (ends - starts) / 2

    overlap_flags = []
    overlap_counts = []
    nearest_timestamps = []
    signed_distances = []
    for window_start, window_end, centre in zip(starts, ends, centres):
        overlap = (event_times >= window_start) & (event_times < window_end)
        overlap_count = int(overlap.sum())
        deltas = (events - centre.to_datetime64()) / np.timedelta64(1, "m")
        if overlap_count:
            overlap_indices = np.flatnonzero(overlap.to_numpy())
            nearest_index = int(
                overlap_indices[np.argmin(np.abs(deltas[overlap_indices]))]
            )
        else:
            nearest_index = int(np.argmin(np.abs(deltas)))

        overlap_flags.append(overlap_count > 0)
        overlap_counts.append(overlap_count)
        nearest_timestamps.append(event_times.iloc[nearest_index])
        signed_distances.append(
            0.0 if overlap_count else float(deltas[nearest_index])
        )

    result["craving_overlap"] = overlap_flags
    result["craving_event_count"] = overlap_counts
    result["nearest_craving_timestamp"] = pd.to_datetime(
        nearest_timestamps, utc=True
    )
    result["minutes_to_nearest_craving"] = signed_distances
    return normalise_feature_dtypes(result)


def enrich_feature_windows(
    features: pd.DataFrame,
    participant_id: str | int,
    sociodemographics: pd.DataFrame,
    cravings: pd.DataFrame,
) -> pd.DataFrame:
    """Append participant sociodemographics and craving annotations."""
    result = append_sociodemographics(
        features, participant_id, sociodemographics
    )
    result = append_craving_annotations(result, participant_id, cravings)
    return normalise_feature_dtypes(result)
