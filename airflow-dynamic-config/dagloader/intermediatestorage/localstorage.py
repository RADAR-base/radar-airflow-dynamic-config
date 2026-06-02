from dagloader.intermediatestorage.storage import Storage
import os
import glob
import re
from typing import Any
import pickle


class LocalStorage(Storage):
    def __init__(self, path: str):
        self.base_path = path
        os.makedirs(self.base_path, exist_ok=True)

    def _path_for(self, key: str, run_id) -> str:
        """Path for a run. When a run_id is present the snapshot is written
        per-run so overlapping runs cannot clobber each other."""
        if run_id is not None:
            safe = re.sub(r'[^0-9A-Za-z_.-]', '_', str(run_id))
            return f"{self.base_path}/{key}__{safe}.pkl"
        return f"{self.base_path}/{key}.pkl"

    def save(self, key: str, data: Any) -> None:
        file_path = self._path_for(key, self._effective_run_id())
        with open(file_path, 'wb') as f:
            pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)

    def load(self, key: str, scoped: bool = True) -> Any:
        run_id = self._effective_run_id()
        if scoped and run_id is not None:
            file_path = self._path_for(key, run_id)
        else:
            file_path = self._latest_global(key)
        if not file_path or not os.path.exists(file_path):
            raise FileNotFoundError(f"No data found for key: {key}")
        with open(file_path, 'rb') as f:
            return pickle.load(f)

    def _latest_global(self, key: str) -> str:
        """Most recent snapshot for a key across all runs (used for
        cross-run state such as backoff)."""
        candidates = glob.glob(f"{self.base_path}/{key}__*.pkl")
        legacy = f"{self.base_path}/{key}.pkl"
        if os.path.exists(legacy):
            candidates.append(legacy)
        if not candidates:
            return legacy
        return max(candidates, key=os.path.getmtime)

    def init(self, **kwargs):
        self.directory_name = kwargs.get('directory_name', 'local_storage')
        self.base_path = os.path.join(self.base_path, self.directory_name)
        os.makedirs(self.base_path, exist_ok=True)
