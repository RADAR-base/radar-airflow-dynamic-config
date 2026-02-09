from dagloader.intermediatestorage.storage import Storage
import os
from typing import Any
import pickle


class LocalStorage(Storage):
    def __init__(self, path: str):
        self.base_path = path
        os.makedirs(self.base_path, exist_ok=True)

    def save(self, key: str, data: Any) -> None:
        file_path = f"{self.base_path}/{self.directory_name}/{key}.pkl"
        with open(file_path, 'wb') as f:
            pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)

    def load(self, key: str) -> Any:
        file_path = f"{self.base_path}/{self.directory_name}/{key}.pkl"
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"No data found for key: {key}")
        with open(file_path, 'rb') as f:
            return pickle.load(f)

    def init(self, **kwargs):
        self.directory_name = kwargs.get('directory_name', 'local_storage')
        self.base_path = os.path.join(self.base_path, self.directory_name)
        os.makedirs(self.base_path, exist_ok=True)