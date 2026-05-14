from dagloader.sensors.kafkasensor import KafkaSensorOperator
from dagloader.sensors.s3sensor import S3SensorOperator


class SensorFactory:
    @staticmethod
    def get_sensor(sensor_type: str, sensor_config: dict, **kwargs):
    	if sensor_type == 'kafka':
    		return KafkaSensorOperator(sensor_config=sensor_config, **kwargs)
    	if sensor_type == 's3':
    		return S3SensorOperator(sensor_config=sensor_config, **kwargs)
    	raise ValueError(f"Unsupported sensor type: {sensor_type}")
