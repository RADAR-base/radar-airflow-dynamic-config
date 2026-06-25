import logging
import os
import sys

_SRC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "src")
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

from mcraving.realtime.pipeline import avro_records_to_feature_matrix
logger = logging.getLogger(__name__)

class PreprocessingTaskProcessor():
    def __init__(self):
        super().__init__()

    def execute(self, data):
        # Implement your custom preprocessing logic here
        # For example, you can perform data cleaning, feature engineering, etc.
        # This is a placeholder implementation and should be replaced with actual logic
        logger.info(f"Preprocessing data size: {len(data)}")
        # combined keys of the data
        preprocessed_data = []
        for key, value in data.items():
            for topic, datum in value.items():
                preprocessed_data += datum
        logger.info(f"Preprocessed data size: {len(preprocessed_data)}")
        preprocessed_data = avro_records_to_feature_matrix(preprocessed_data)
        logger.info(f"Preprocessed data final size: {len(preprocessed_data)}")
        return preprocessed_data
