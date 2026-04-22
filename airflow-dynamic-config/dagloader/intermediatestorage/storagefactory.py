from dagloader.intermediatestorage.storage import Storage
from dagloader.intermediatestorage.localstorage import LocalStorage
from dagloader.intermediatestorage.s3storage import S3Storage


class StorageFactory:
    @staticmethod
    def get_storage(storage_type: str, **kwargs) -> Storage:
        if storage_type == 'local':
            return LocalStorage(**kwargs)
        elif storage_type == 's3':
            return S3Storage(**kwargs)
        else:
            raise ValueError(f"Unsupported storage type: {storage_type}")