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
`backoff_key` value was last fired within the backoff window. State is
persisted in the pipeline's intermediate storage under
`<action_name>_backoff` as a dict of `{key_value: iso_timestamp}`.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


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
        self.enabled = (
            bool(self.config)
            and self.config.get('enabled', True) is not False
            and bool(self.config.get('backoff_key'))
            and self.config.get('interval') is not None
        )
        self.backoff_key: Optional[str] = self.config.get('backoff_key')
        self.strategy: str = (
            self.config.get('backoff_strategy', 'fixed') or 'fixed'
        ).lower()
        self.window: Optional[timedelta] = None
        if self.enabled:
            raw_seconds = self.config.get('interval')
            if isinstance(raw_seconds, bool) or not isinstance(
                raw_seconds, (int, float)
            ) or raw_seconds < 0:
                logger.warning(
                    f"Backoff disabled for action '{action_name}': "
                    f"'interval' must be a non-negative number of seconds, "
                    f"got {raw_seconds!r}."
                )
                self.enabled = False
            else:
                self.window = timedelta(seconds=float(raw_seconds))

    def _load_state(self) -> Dict[str, datetime]:
        try:
            raw = self.storage.load(self.storage_key)
        except FileNotFoundError:
            return {}
        if not isinstance(raw, dict):
            return {}
        state: Dict[str, datetime] = {}
        for key, value in raw.items():
            if isinstance(value, datetime):
                state[str(key)] = value
            elif isinstance(value, str):
                try:
                    state[str(key)] = datetime.fromisoformat(value)
                except ValueError:
                    continue
        return state

    def _save_state(self, state: Dict[str, datetime]) -> None:
        serialised = {key: ts.isoformat() for key, ts in state.items()}
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

    def apply(self, items: List[Any]) -> List[Any]:
        if not self.enabled or not items:
            return items

        state = self._load_state()
        logger.debug(
            f"Old Backoff state for action '{self.action_name}': "
            f"{ {k: v.isoformat() for k, v in state.items()} }"
        )
        now = datetime.now(timezone.utc)
        passed: List[Any] = []
        skipped = 0

        for item in items:
            key_value = self._extract_key(item)
            if key_value is None:
                passed.append(item)
                continue
            last_fired = state.get(key_value)
            if last_fired is not None and now < self._next_allowed(last_fired):
                skipped += 1
                continue
            state[key_value] = now
            passed.append(item)

        if skipped:
            logger.info(
                f"Backoff for action '{self.action_name}' skipped {skipped} "
                f"of {len(items)} items (window={self.window})."
            )
        if passed:
            self._save_state(state)
        logger.debug(
            f"New Backoff state for action '{self.action_name}': "
            f"{ {k: v.isoformat() for k, v in state.items()} }"
        )
        return passed
