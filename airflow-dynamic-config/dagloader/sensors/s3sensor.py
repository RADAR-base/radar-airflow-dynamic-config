from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from dagloader.sensors.sensoroperator import ConfigurableSensorOperator
import logging
import re

logger = logging.getLogger(__name__)


class S3SensorOperator(ConfigurableSensorOperator):
    def _resolve_config(self):
        self.bucket = self.get_config('bucket')
        self.prefix = self.get_config('prefix', '')
        self.regex = self.get_config('regex')
        self.aws_conn_id = self.get_config('conn_id', 'aws_default')

    def poke(self, context):
        if not self.bucket:
            logger.warning("S3 sensor has no bucket configured.")
            return False

        hook = S3Hook(aws_conn_id=self.aws_conn_id)
        keys = hook.list_keys(bucket_name=self.bucket, prefix=self.prefix) or []
        if not keys:
            return False

        if self.regex:
            pattern = re.compile(self.regex)
            keys = [key for key in keys if pattern.search(key)]
            if not keys:
                return False

        self.save_to_storage(keys)
        return True
