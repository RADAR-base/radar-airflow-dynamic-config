import logging
from src.mcraving.realtime.pipeline import avro_records_to_feature_matrix
logger = logging.getLogger(__name__)

class PreprocessingTaskProcessor():
    def __init__(self):
        super().__init__()

    def execute(self, data):
        # Implement your custom preprocessing logic here
        # For example, you can perform data cleaning, feature engineering, etc.
        # This is a placeholder implementation and should be replaced with actual logic
        logger.info(f"Preprocessing data: {data}")
        # combined keys of the data
        preprocessed_data = []
        for key, value in data.items():
            for topic, datum in value.items():
                preprocessed_data += datum
        return avro_records_to_feature_matrix(preprocessed_data)
