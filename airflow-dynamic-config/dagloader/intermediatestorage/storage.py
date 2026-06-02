from typing import Any, Optional
from abc import ABC, abstractmethod


def _airflow_run_id() -> Optional[str]:
    """Best-effort lookup of the current DAG run id from Airflow's task
    context. Returns None when called outside a running task (in which case
    storage falls back to unscoped/global behaviour)."""
    try:
        from airflow.sdk import get_current_context
    except Exception:
        try:
            from airflow.operators.python import get_current_context
        except Exception:
            return None
    try:
        ctx = get_current_context()
    except Exception:
        return None
    if not ctx:
        return None
    run_id = ctx.get("run_id")
    if run_id:
        return str(run_id)
    dag_run = ctx.get("dag_run")
    rid = getattr(dag_run, "run_id", None)
    return str(rid) if rid else None


class Storage(ABC):
    # Optional explicit override, mainly for tests. In normal operation it is
    # left as None and the run id is resolved automatically from Airflow's
    # current task context, so callers just use save()/load() as before.
    run_id: Optional[str] = None

    def set_run_id(self, run_id: Optional[str]) -> None:
        """Force the run id (e.g. in tests). Normally unnecessary."""
        self.run_id = run_id

    def _effective_run_id(self) -> Optional[str]:
        """The run id to scope by: an explicit override if set, otherwise the
        DAG run currently executing this task."""
        if self.run_id is not None:
            return self.run_id
        return _airflow_run_id()

    @abstractmethod
    def save(self, key: str, data: Any) -> None:
        pass

    @abstractmethod
    def load(self, key: str, scoped: bool = True) -> Any:
        """Load data for a key.

        When ``scoped`` is True the lookup is restricted to the current DAG
        run (resolved automatically); otherwise the most recent value across
        all runs is returned (used for cross-run state such as backoff)."""
        pass

    @abstractmethod
    def init(self, **kwargs):
        pass
