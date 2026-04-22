from dagloader.intermediatestorage.storage import Storage
from dagloader.intermediatestorage.localstorage import LocalStorage
from dagloader.intermediatestorage.s3storage import S3Storage
from dagloader.intermediatestorage.sqlstorage import SQLStorage


class StorageFactory:
    @staticmethod
    def get_storage(storage_type: str, **kwargs) -> Storage:
        storage_type_normalized = (storage_type or '').lower()
        if storage_type_normalized == 'local':
            return LocalStorage(**kwargs)
        elif storage_type_normalized == 's3':
            return S3Storage(**kwargs)
        elif storage_type_normalized == 'sql':
            return SQLStorage(**kwargs)
        else:
            raise ValueError(f"Unsupported storage type: {storage_type}")
