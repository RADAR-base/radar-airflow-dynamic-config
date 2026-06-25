"""Window selection and participant-level validation for craving models."""

from __future__ import annotations

from collections.abc import Callable, Iterable

import numpy as np
import pandas as pd
from sklearn.base import clone
from sklearn.compose import ColumnTransformer
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    balanced_accuracy_score,
    classification_report,
    confusion_matrix,
    roc_auc_score,
)
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer, OneHotEncoder
from xgboost import XGBClassifier


NON_PREDICTOR_COLUMNS = {
    "participant_id",
    "window_start",
    "window_end",
    "window_start_minute",
    "window_end_minute",
    "complete_window",
    "sufficient_data",
    "acc_usable_fraction",
    "eda_scl_usable_fraction",
    "eda_mse_usable_fraction",
    "temp_usable_fraction",
    "hr_usable_fraction",
    "minimum_sensor_usable_fraction",
    "craving_overlap",
    "craving_event_count",
    "nearest_craving_timestamp",
    "minutes_to_nearest_craving",
    "minutes_to_nearest_craving_or_use",
    "label",
    "selection",
    "latest_avro_start",
    "latest_avro_end",
    "device_serial",
}

USABLE_FRACTION_COLUMNS = {
    "acc": "acc_usable_fraction",
    "eda_scl": "eda_scl_usable_fraction",
    "eda_mse": "eda_mse_usable_fraction",
    "temp": "temp_usable_fraction",
    "hr": "hr_usable_fraction",
}


def _prepare_numeric(values) -> np.ndarray:
    """Convert pandas nullable numeric values to a float array with np.nan."""
    frame = pd.DataFrame(values)
    return frame.apply(pd.to_numeric, errors="coerce").to_numpy(
        dtype=float,
        na_value=np.nan,
    )


def _prepare_categorical(values) -> np.ndarray:
    """Convert mixed nullable categoricals to uniform strings for encoding."""
    frame = pd.DataFrame(values)
    return (
        frame.astype("string")
        .fillna("__missing__")
        .to_numpy(dtype=str)
    )


def filter_windows_by_quality(
    features: pd.DataFrame,
    *,
    minimum_usable_fraction: float | None = None,
    require_complete: bool = False,
    modality_minimums: dict[str, float] | None = None,
) -> pd.DataFrame:
    """
    Retain windows satisfying configurable signal-quality thresholds.

    ``minimum_usable_fraction`` applies to every available sensor channel.
    ``modality_minimums`` can override it for named channels: ``acc``,
    ``eda_scl``, ``eda_mse``, ``temp``, and ``hr``. Set a threshold to ``None``
    to ignore that channel. ``require_complete`` additionally requires the
    strict ``complete_window`` flag.
    """
    if minimum_usable_fraction is not None and not (
        0 <= minimum_usable_fraction <= 1
    ):
        raise ValueError(
            "minimum_usable_fraction must lie between zero and one."
        )

    unknown_modalities = set(modality_minimums or {}).difference(
        USABLE_FRACTION_COLUMNS
    )
    if unknown_modalities:
        raise ValueError(
            f"Unknown modality thresholds: {sorted(unknown_modalities)}"
        )

    eligible = pd.Series(True, index=features.index)
    if require_complete:
        if "complete_window" not in features:
            raise KeyError(
                "require_complete=True needs the complete_window column."
            )
        eligible &= features["complete_window"].fillna(False).astype(bool)

    thresholds = {
        modality: minimum_usable_fraction
        for modality in USABLE_FRACTION_COLUMNS
    }
    thresholds.update(modality_minimums or {})
    for modality, threshold in thresholds.items():
        if threshold is None:
            continue
        if not 0 <= threshold <= 1:
            raise ValueError(
                f"The {modality} usable-fraction threshold must lie "
                "between zero and one."
            )
        column = USABLE_FRACTION_COLUMNS[modality]
        if column not in features:
            raise KeyError(
                f"Quality filtering needs the {column} column. Re-run "
                "04.1-features.ipynb with the quality-enabled pipeline."
            )
        eligible &= features[column].ge(threshold).fillna(False)

    return features.loc[eligible].copy()


def _participant_events(
    events: pd.DataFrame,
    participant_id: str,
    participant_column: str = "participant_id",
    timestamp_column: str = "timestamp",
) -> pd.Series:
    if events is None or events.empty:
        return pd.Series([], dtype="datetime64[ns, UTC]")
    selected = events.loc[
        events[participant_column].astype(str) == str(participant_id),
        timestamp_column,
    ]
    return (
        pd.to_datetime(selected, utc=True)
        .dropna()
        .drop_duplicates()
        .sort_values()
        .reset_index(drop=True)
    )


def _first_events_in_clusters(
    event_times: pd.Series,
    *,
    cluster_gap_minutes: float,
) -> pd.Series:
    """Keep the first event from each run of close-together events."""
    if event_times.empty:
        return event_times
    if cluster_gap_minutes < 0:
        raise ValueError("cluster_gap_minutes must be non-negative.")

    retained = [event_times.iloc[0]]
    previous_event = event_times.iloc[0]
    for event_time in event_times.iloc[1:]:
        gap_minutes = (event_time - previous_event).total_seconds() / 60
        if gap_minutes > cluster_gap_minutes:
            retained.append(event_time)
        previous_event = event_time
    return pd.Series(retained, dtype="datetime64[ns, UTC]")


def select_positive_windows(
    features: pd.DataFrame,
    cravings: pd.DataFrame,
    *,
    method: str = "any_overlap",
    event_cluster_gap_minutes: float = 40.0,
    minimum_usable_fraction: float | None = None,
    require_complete: bool = False,
    modality_minimums: dict[str, float] | None = None,
) -> pd.DataFrame:
    """
    Select craving-positive windows.

    ``any_overlap`` keeps every window containing at least one craving.
    ``one_per_event`` assigns one window to each craving: the overlapping
    window whose centre is closest to the event. A window is returned once
    even if it is selected for multiple near-duplicate events.
    ``first_per_event_cluster`` first clusters consecutive craving reports
    separated by no more than ``event_cluster_gap_minutes`` and only assigns a
    window to the first event in each cluster.
    """
    allowed_methods = {
        "any_overlap",
        "one_per_event",
        "first_per_event_cluster",
    }
    if method not in allowed_methods:
        raise ValueError(f"method must be one of {sorted(allowed_methods)}.")

    frame = filter_windows_by_quality(
        features,
        minimum_usable_fraction=minimum_usable_fraction,
        require_complete=require_complete,
        modality_minimums=modality_minimums,
    )
    frame["participant_id"] = frame["participant_id"].astype(str)
    starts = pd.to_datetime(frame["window_start"], utc=True)
    ends = pd.to_datetime(frame["window_end"], utc=True)

    if method == "any_overlap":
        if "craving_overlap" in frame:
            selected = frame["craving_overlap"].fillna(False)
        else:
            selected = pd.Series(False, index=frame.index)
            for participant_id, indices in frame.groupby("participant_id").groups.items():
                event_times = _participant_events(cravings, participant_id)
                if event_times.empty:
                    continue
                participant_starts = starts.loc[indices]
                participant_ends = ends.loc[indices]
                selected.loc[indices] = [
                    ((event_times >= start) & (event_times < end)).any()
                    for start, end in zip(participant_starts, participant_ends)
                ]
        result = frame.loc[selected].copy()
    else:
        selected_indices = []
        centres = starts + (ends - starts) / 2
        for participant_id, indices in frame.groupby("participant_id").groups.items():
            event_times = _participant_events(cravings, participant_id)
            if method == "first_per_event_cluster":
                event_times = _first_events_in_clusters(
                    event_times,
                    cluster_gap_minutes=event_cluster_gap_minutes,
                )
            for event_time in event_times:
                overlaps = indices[
                    (starts.loc[indices] <= event_time)
                    & (event_time < ends.loc[indices])
                ]
                if len(overlaps) == 0:
                    continue
                distances = (centres.loc[overlaps] - event_time).abs()
                selected_indices.append(distances.idxmin())
        result = frame.loc[pd.Index(selected_indices).drop_duplicates()].copy()

    result["label"] = 1
    result["selection"] = f"positive:{method}"
    return result.sort_values(["participant_id", "window_start"])


def window_distance_to_events_minutes(
    features: pd.DataFrame,
    event_frames: Iterable[pd.DataFrame],
) -> pd.Series:
    """
    Return each window's distance to the closest event.

    Distance is zero for overlap; otherwise it is the gap from the nearest
    window edge to the event, in minutes.
    """
    distances = pd.Series(np.inf, index=features.index, dtype=float)
    participant_ids = features["participant_id"].astype(str)
    starts = pd.to_datetime(features["window_start"], utc=True)
    ends = pd.to_datetime(features["window_end"], utc=True)

    available_events = [
        frame.loc[:, ["participant_id", "timestamp"]]
        for frame in event_frames
        if frame is not None and not frame.empty
    ]
    if not available_events:
        return distances
    combined = pd.concat(available_events, ignore_index=True)
    combined["participant_id"] = combined["participant_id"].astype(str)
    combined["timestamp"] = pd.to_datetime(combined["timestamp"], utc=True)

    for participant_id, indices in features.groupby(participant_ids).groups.items():
        event_times = _participant_events(combined, participant_id)
        if event_times.empty:
            continue
        event_array = event_times.to_numpy(dtype="datetime64[ns]")
        for index in indices:
            start = starts.loc[index].to_datetime64()
            end = ends.loc[index].to_datetime64()
            gaps = np.where(
                event_array < start,
                (start - event_array) / np.timedelta64(1, "m"),
                np.where(
                    event_array >= end,
                    (event_array - end) / np.timedelta64(1, "m"),
                    0.0,
                ),
            )
            distances.loc[index] = float(np.min(gaps))
    return distances


def select_distant_negative_windows(
    features: pd.DataFrame,
    cravings: pd.DataFrame,
    use_events: pd.DataFrame,
    *,
    minimum_distance_hours: float = 3.0,
    minimum_usable_fraction: float | None = None,
    require_complete: bool = False,
    modality_minimums: dict[str, float] | None = None,
) -> pd.DataFrame:
    """Select windows at least the requested distance from craving and use."""
    features = filter_windows_by_quality(
        features,
        minimum_usable_fraction=minimum_usable_fraction,
        require_complete=require_complete,
        modality_minimums=modality_minimums,
    )
    distances = window_distance_to_events_minutes(
        features, [cravings, use_events]
    )
    result = features.loc[distances >= minimum_distance_hours * 60].copy()
    result["minutes_to_nearest_craving_or_use"] = distances.loc[result.index]
    result["label"] = 0
    result["selection"] = "negative:distant"
    return result.sort_values(["participant_id", "window_start"])


def select_no_craving_report_windows(
    features: pd.DataFrame,
    no_craving_reports: pd.DataFrame,
) -> pd.DataFrame:
    """
    Placeholder for retrospective self-reported no-craving intervals.

    The intended input has participant ID, report timestamp, and lookback
    duration (or an explicit interval start). Selection should retain windows
    fully contained in each reported no-craving interval.
    """
    raise NotImplementedError(
        "No-craving report selection awaits the dataframe schema. Expected "
        "participant_id, report_timestamp, and lookback duration or interval_start."
    )


def balance_selected_windows(
    positives: pd.DataFrame,
    negatives: pd.DataFrame,
    *,
    method: str = "equal",
    by_participant: bool = True,
    random_state: int = 12345,
) -> pd.DataFrame:
    """Combine all cases or randomly downsample negatives to positive counts."""
    if method not in {"equal", "all"}:
        raise ValueError("method must be 'equal' or 'all'.")
    if method == "all":
        return pd.concat([positives, negatives], ignore_index=True).sort_values(
            ["participant_id", "window_start"]
        )

    if by_participant:
        sampled_positives = []
        sampled_negatives = []
        for participant_id, participant_positives in positives.groupby(
            positives["participant_id"].astype(str)
        ):
            participant_negatives = negatives.loc[
                negatives["participant_id"].astype(str) == participant_id
            ]
            count = min(len(participant_positives), len(participant_negatives))
            if count == 0:
                continue
            sampled_positives.append(
                participant_positives.sample(
                    n=count, random_state=random_state
                )
            )
            sampled_negatives.append(
                participant_negatives.sample(
                    n=count, random_state=random_state
                )
            )
        positives = (
            pd.concat(sampled_positives)
            if sampled_positives
            else positives.iloc[0:0].copy()
        )
        sampled_negatives = (
            pd.concat(sampled_negatives)
            if sampled_negatives
            else negatives.iloc[0:0].copy()
        )
    else:
        count = min(len(positives), len(negatives))
        positives = positives.sample(n=count, random_state=random_state)
        sampled_negatives = negatives.sample(n=count, random_state=random_state)

    return pd.concat([positives, sampled_negatives], ignore_index=True).sort_values(
        ["participant_id", "window_start"]
    )


def make_analysis_dataset(
    features: pd.DataFrame,
    cravings: pd.DataFrame,
    use_events: pd.DataFrame,
    *,
    positive_method: str = "any_overlap",
    negative_method: str = "distant",
    minimum_distance_hours: float = 3.0,
    positive_event_cluster_gap_minutes: float = 40.0,
    positive_minimum_usable_fraction: float | None = None,
    negative_minimum_usable_fraction: float | None = None,
    positive_require_complete: bool = False,
    negative_require_complete: bool = False,
    positive_modality_minimums: dict[str, float] | None = None,
    negative_modality_minimums: dict[str, float] | None = None,
    balance_method: str = "equal",
    balance_by_participant: bool = True,
    random_state: int = 12345,
) -> pd.DataFrame:
    """Select positive and negative windows for one modeling experiment."""
    positives = select_positive_windows(
        features,
        cravings,
        method=positive_method,
        event_cluster_gap_minutes=positive_event_cluster_gap_minutes,
        minimum_usable_fraction=positive_minimum_usable_fraction,
        require_complete=positive_require_complete,
        modality_minimums=positive_modality_minimums,
    )
    if negative_method != "distant":
        raise NotImplementedError(
            "Only negative_method='distant' is available until the "
            "no-craving-report dataframe schema is supplied."
        )
    negatives = select_distant_negative_windows(
        features,
        cravings,
        use_events,
        minimum_distance_hours=minimum_distance_hours,
        minimum_usable_fraction=negative_minimum_usable_fraction,
        require_complete=negative_require_complete,
        modality_minimums=negative_modality_minimums,
    )
    return balance_selected_windows(
        positives,
        negatives,
        method=balance_method,
        by_participant=balance_by_participant,
        random_state=random_state,
    ).reset_index(drop=True)


def _participant_sort_key(participant_id: str):
    text = str(participant_id)
    try:
        return (0, int(text))
    except ValueError:
        return (1, text)


def ordered_participant_holdout(
    data: pd.DataFrame,
    *,
    train_fraction: float = 0.75,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Use the lowest participant IDs for training and the rest for testing."""
    if not 0 < train_fraction < 1:
        raise ValueError("train_fraction must lie between zero and one.")
    participants = sorted(
        data["participant_id"].astype(str).unique(),
        key=_participant_sort_key,
    )
    if len(participants) < 2:
        raise ValueError("At least two participants are required.")
    train_count = min(
        len(participants) - 1,
        max(1, int(np.floor(len(participants) * train_fraction))),
    )
    train_participants = set(participants[:train_count])
    train = data.loc[
        data["participant_id"].astype(str).isin(train_participants)
    ].copy()
    test = data.loc[
        ~data["participant_id"].astype(str).isin(train_participants)
    ].copy()
    return train, test


def participant_folds(
    data: pd.DataFrame,
    *,
    test_participants_per_fold: int = 1,
) -> Iterable[tuple[list[str], list[str]]]:
    """Yield deterministic leave-one/leave-N-participant-out folds."""
    if test_participants_per_fold < 1:
        raise ValueError("test_participants_per_fold must be at least one.")
    participants = sorted(
        data["participant_id"].astype(str).unique(),
        key=_participant_sort_key,
    )
    if len(participants) <= test_participants_per_fold:
        raise ValueError("Each fold must leave at least one training participant.")
    for start in range(0, len(participants), test_participants_per_fold):
        test = participants[start : start + test_participants_per_fold]
        train = [participant for participant in participants if participant not in test]
        if train and test:
            yield train, test


def default_model(random_state: int = 12345) -> XGBClassifier:
    """Return the notebook's default binary classifier."""
    return XGBClassifier(
        n_estimators=200,
        max_depth=5,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        eval_metric="logloss",
        random_state=random_state,
    )


def build_model_pipeline(
    data: pd.DataFrame,
    estimator=None,
) -> tuple[Pipeline, list[str]]:
    """Construct one-hot preprocessing and a classifier without label leakage."""
    predictor_columns = [
        column
        for column in data.columns
        if column not in NON_PREDICTOR_COLUMNS
        and not pd.api.types.is_datetime64_any_dtype(data[column])
    ]
    categorical = [
        column
        for column in predictor_columns
        if pd.api.types.is_bool_dtype(data[column])
        or not pd.api.types.is_numeric_dtype(data[column])
    ]
    numeric = [
        column for column in predictor_columns if column not in categorical
    ]
    preprocessing = ColumnTransformer(
        [
            (
                "numeric",
                Pipeline(
                    [
                        (
                            "to_float",
                            FunctionTransformer(
                                _prepare_numeric,
                                validate=False,
                            ),
                        ),
                        (
                            "impute",
                            SimpleImputer(
                                strategy="median",
                                keep_empty_features=True,
                            ),
                        ),
                    ]
                ),
                numeric,
            ),
            (
                "categorical",
                Pipeline(
                    [
                        (
                            "to_string",
                            FunctionTransformer(
                                _prepare_categorical,
                                validate=False,
                            ),
                        ),
                        (
                            "encode",
                            OneHotEncoder(
                                handle_unknown="ignore",
                                sparse_output=False,
                            ),
                        ),
                    ]
                ),
                categorical,
            ),
        ],
        remainder="drop",
    )
    pipeline = Pipeline(
        [
            ("preprocessing", preprocessing),
            ("model", estimator or default_model()),
        ]
    )
    return pipeline, predictor_columns


def evaluate_predictions(
    y_true,
    probabilities,
    *,
    threshold: float = 0.5,
) -> dict:
    """Return scalar and detailed binary classification metrics."""
    predictions = np.asarray(probabilities) >= threshold
    y_true = np.asarray(y_true, dtype=int)
    metrics = {
        "n": len(y_true),
        "positive_count": int(y_true.sum()),
        "accuracy": accuracy_score(y_true, predictions),
        "balanced_accuracy": balanced_accuracy_score(y_true, predictions),
        "confusion_matrix": confusion_matrix(
            y_true, predictions, labels=[0, 1]
        ),
        "classification_report": classification_report(
            y_true, predictions, labels=[0, 1], zero_division=0
        ),
    }
    if np.unique(y_true).size == 2:
        metrics["roc_auc"] = roc_auc_score(y_true, probabilities)
        metrics["average_precision"] = average_precision_score(
            y_true, probabilities
        )
    else:
        metrics["roc_auc"] = np.nan
        metrics["average_precision"] = np.nan
    return metrics


def fit_and_evaluate_holdout(
    data: pd.DataFrame,
    *,
    train_fraction: float = 0.75,
    estimator=None,
) -> tuple[Pipeline, pd.DataFrame, dict]:
    """Fit on the lowest participant IDs and evaluate on the remainder."""
    train, test = ordered_participant_holdout(
        data, train_fraction=train_fraction
    )
    model, predictors = build_model_pipeline(data, estimator=estimator)
    model.fit(train[predictors], train["label"])
    probabilities = model.predict_proba(test[predictors])[:, 1]
    predictions = test[
        ["participant_id", "window_start", "window_end", "label"]
    ].copy()
    predictions["probability"] = probabilities
    metrics = evaluate_predictions(test["label"], probabilities)
    metrics["train_participants"] = sorted(
        train["participant_id"].astype(str).unique(), key=_participant_sort_key
    )
    metrics["test_participants"] = sorted(
        test["participant_id"].astype(str).unique(), key=_participant_sort_key
    )
    return model, predictions, metrics


def cross_validate_participants(
    data: pd.DataFrame,
    *,
    test_participants_per_fold: int = 1,
    estimator_factory: Callable[[], object] = default_model,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run deterministic leave-one/leave-N-participant-out validation."""
    all_predictions = []
    fold_metrics = []
    for fold, (train_ids, test_ids) in enumerate(
        participant_folds(
            data,
            test_participants_per_fold=test_participants_per_fold,
        ),
        start=1,
    ):
        train = data.loc[data["participant_id"].astype(str).isin(train_ids)]
        test = data.loc[data["participant_id"].astype(str).isin(test_ids)]
        model, predictors = build_model_pipeline(
            data, estimator=clone(estimator_factory())
        )
        model.fit(train[predictors], train["label"])
        probabilities = model.predict_proba(test[predictors])[:, 1]
        metrics = evaluate_predictions(test["label"], probabilities)
        fold_metrics.append(
            {
                "fold": fold,
                "train_participants": train_ids,
                "test_participants": test_ids,
                **{
                    key: value
                    for key, value in metrics.items()
                    if key not in {"confusion_matrix", "classification_report"}
                },
            }
        )
        predictions = test[
            ["participant_id", "window_start", "window_end", "label"]
        ].copy()
        predictions["fold"] = fold
        predictions["probability"] = probabilities
        all_predictions.append(predictions)

    return (
        pd.concat(all_predictions, ignore_index=True),
        pd.DataFrame(fold_metrics),
    )
