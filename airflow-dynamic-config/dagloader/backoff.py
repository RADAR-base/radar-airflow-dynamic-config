"""Backoff filter for actions.

An action may declare a `backoff` block:

    backoff:
      enabled: true
      interval: 600             # seconds
      backoff_key: "participants_id"
      backoff_strategy: "fixed"
      max_messages: 3
      max_messages_units: "day"

When applied to a list of items, the filter drops any item whose
`backoff_key` value either fired within the `interval` window (minimum
gap between two consecutive messages) or has already fired
`max_messages` times within the rolling `max_messages_units` window
(e.g. at most 3 messages per day; `max_messages_units` defaults to
"day" when omitted). Either limit may be configured on its own. State is
persisted in the pipeline's intermediate storage under
`<action_name>_backoff` as a dict of `{key_value: [iso_timestamp, ...]}`.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Supported units for `max_messages_units`, mapped to their duration.
_MESSAGE_UNITS: Dict[str, timedelta] = {
    "second": timedelta(seconds=1),
    "seconds": timedelta(seconds=1),
    "minute": timedelta(minutes=1),
    "minutes": timedelta(minutes=1),
    "hour": timedelta(hours=1),
    "hours": timedelta(hours=1),
    "day": timedelta(days=1),
    "days": timedelta(days=1),
    "week": timedelta(weeks=1),
    "weeks": timedelta(weeks=1),
}


class BackoffFilter:
    """Filter items whose `backoff_key` value is still within backoff.

    The filter is a no-op when the backoff block is missing, disabled,
    or `backoff_key` is unset. State is loaded lazily and only saved
    when at least one key passes the filter.
    """

    def __init__(self, action_name: str, backoff_config: Optional[dict],
                 intermediate_storage):
        self.action_name = action_name
        self.storage = intermediate_storage
        self.storage_key = f"{action_name}_backoff"
        self.config = backoff_config or {}
        self.backoff_key: Optional[str] = self.config.get('backoff_key')
        self.strategy: str = (
            self.config.get('backoff_strategy', 'fixed') or 'fixed'
        ).lower()
        self.window: Optional[timedelta] = None
        self.max_messages: Optional[int] = None
        self.message_window: Optional[timedelta] = None
        self.enabled = (
            bool(self.config)
            and self.config.get('enabled', True) is not False
            and bool(self.backoff_key)
        )
        if self.enabled:
            self._parse_interval(action_name)
            self._parse_max_messages(action_name)
            # At least one limit must be configured for the filter to do
            # anything; otherwise treat it as disabled.
            if self.window is None and self.max_messages is None:
                logger.warning(
                    f"Backoff disabled for action '{action_name}': configure "
                    f"'interval' and/or 'max_messages' with "
                    f"'max_messages_units'."
                )
                self.enabled = False

    def _parse_interval(self, action_name: str) -> None:
        raw_seconds = self.config.get('interval')
        if raw_seconds is None:
            return
        if isinstance(raw_seconds, bool) or not isinstance(
            raw_seconds, (int, float)
        ) or raw_seconds < 0:
            logger.warning(
                f"Backoff for action '{action_name}': 'interval' must be a "
                f"non-negative number of seconds, got {raw_seconds!r}; "
                f"ignoring it."
            )
            return
        self.window = timedelta(seconds=float(raw_seconds))

    def _parse_max_messages(self, action_name: str) -> None:
        raw_max = self.config.get('max_messages')
        if raw_max is None:
            return
        if isinstance(raw_max, bool) or not isinstance(raw_max, int) \
                or raw_max <= 0:
            logger.warning(
                f"Backoff for action '{action_name}': 'max_messages' must be "
                f"a positive integer, got {raw_max!r}; ignoring it."
            )
            return
        raw_units = self.config.get('max_messages_units')
        if raw_units is None:
            # Default to a daily cap when the unit is omitted.
            unit = _MESSAGE_UNITS['day']
        else:
            unit = _MESSAGE_UNITS.get(str(raw_units).lower())
            if unit is None:
                logger.warning(
                    f"Backoff for action '{action_name}': 'max_messages_units' "
                    f"must be one of {sorted(set(_MESSAGE_UNITS))}, got "
                    f"{raw_units!r}; ignoring 'max_messages'."
                )
                return
        self.max_messages = raw_max
        self.message_window = unit

    @staticmethod
    def _coerce_datetime(value: Any) -> Optional[datetime]:
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value)
            except ValueError:
                return None
        return None

    def _load_state(self) -> Dict[str, List[datetime]]:
        try:
            # Backoff state is intentionally cross-run: it tracks when each
            # key fired so it can rate-limit across DAG runs. Load it
            # unscoped so a new run sees the previous runs' state.
            raw = self.storage.load(self.storage_key, scoped=False)
        except FileNotFoundError:
            return {}
        if not isinstance(raw, dict):
            return {}
        state: Dict[str, List[datetime]] = {}
        for key, value in raw.items():
            # Newer state stores a list of timestamps per key; older state
            # stored a single timestamp. Accept both for backward
            # compatibility.
            values = value if isinstance(value, list) else [value]
            timestamps = [
                ts for ts in (self._coerce_datetime(v) for v in values)
                if ts is not None
            ]
            if timestamps:
                state[str(key)] = sorted(timestamps)
        return state

    def _save_state(self, state: Dict[str, List[datetime]]) -> None:
        serialised = {
            key: [ts.isoformat() for ts in timestamps]
            for key, timestamps in state.items()
        }
        self.storage.save(self.storage_key, serialised)

    def _next_allowed(self, last_fired: datetime) -> datetime:
        # Strategy dispatch lives here so additional strategies (e.g.
        # exponential) can be added without touching the filter loop.
        if self.strategy == "fixed":
            return last_fired + (self.window or timedelta(0))
        logger.warning(
            f"Unknown backoff_strategy '{self.strategy}' for action "
            f"'{self.action_name}'; falling back to 'fixed'."
        )
        return last_fired + (self.window or timedelta(0))

    def _extract_key(self, item: Any) -> Optional[str]:
        if not isinstance(item, dict):
            return None
        value = item.get(self.backoff_key)
        if value is None:
            return None
        return str(value)

    def _is_backed_off(self, timestamps: List[datetime], now: datetime) -> bool:
        """Return True if the key should be skipped given its fire history."""
        if not timestamps:
            return False
        # Interval check: minimum gap since the most recent message.
        if self.window is not None and now < self._next_allowed(timestamps[-1]):
            return True
        # Max-messages check: cap within the rolling unit window.
        if self.max_messages is not None and self.message_window is not None:
            cutoff = now - self.message_window
            recent = sum(1 for ts in timestamps if ts > cutoff)
            if recent >= self.max_messages:
                return True
        return False

    def _prune(self, timestamps: List[datetime], now: datetime) -> List[datetime]:
        """Drop timestamps no longer needed by any active limit."""
        if not timestamps:
            return timestamps
        # Keep anything within the message window for the cap, and always keep
        # the most recent timestamp for the interval check.
        kept = timestamps
        if self.message_window is not None:
            cutoff = now - self.message_window
            kept = [ts for ts in timestamps if ts > cutoff]
        if not kept:
            kept = [timestamps[-1]]
        return kept

    def apply(self, items: List[Any]) -> List[Any]:
        if not self.enabled or not items:
            return items

        state = self._load_state()
        logger.debug(
            f"Old Backoff state for action '{self.action_name}': "
            f"{ {k: [t.isoformat() for t in v] for k, v in state.items()} }"
        )
        now = datetime.now(timezone.utc)
        passed: List[Any] = []
        skipped = 0

        for item in items:
            key_value = self._extract_key(item)
            if key_value is None:
                passed.append(item)
                continue
            timestamps = self._prune(state.get(key_value, []), now)
            if self._is_backed_off(timestamps, now):
                skipped += 1
                state[key_value] = timestamps
                continue
            timestamps.append(now)
            state[key_value] = timestamps
            passed.append(item)

        if skipped:
            logger.info(
                f"Backoff for action '{self.action_name}' skipped {skipped} "
                f"of {len(items)} items (interval={self.window}, "
                f"max_messages={self.max_messages} per {self.message_window})."
            )
        if passed:
            self._save_state(state)
        logger.debug(
            f"New Backoff state for action '{self.action_name}': "
            f"{ {k: [t.isoformat() for t in v] for k, v in state.items()} }"
        )
        return passed
