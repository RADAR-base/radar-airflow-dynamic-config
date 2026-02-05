from dagloader.intermediatestorage.storage import Storage
from typing import Any


class S3Storage(Storage):
    def __init__(self, bucket_name: str, aws_access_key: str, aws_secret_key: str):
        self.bucket_name = bucket_name
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        # Initialize S3 client here (e.g., using boto3)

    def save(self, key: str, data: Any) -> None:
        # Code to save data to S3 bucket
        pass

    def load(self, key: str) -> Any:
        # Code to load data from S3 bucket
        pass