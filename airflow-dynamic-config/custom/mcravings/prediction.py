import random
import pandas as pd
import logging
from airflow.exceptions import AirflowSkipException
logger = logging.getLogger(__name__)
class PredictionTaskProcessor():
    def __init__(self):
        super().__init__()

    def execute(self, data):
        #logger.info(f"Predicting with data: {data}")
        dfs = []
        for data_sources in data.values():
            if isinstance(data_sources, list):
                dfs.append(pd.DataFrame(data_sources))
                continue
            for _, topic_data in data_sources.items():
                dfs.append(pd.DataFrame(topic_data))

        # No upstream feature frames at all: skip so the notification action is
        # skipped too instead of failing on an empty concat.
        dfs = [frame for frame in dfs if not frame.empty]
        if not dfs:
            raise AirflowSkipException("No feature data to predict on.")

        df = pd.concat(dfs, ignore_index=True)
        logger.info(f"columns: {df.columns.tolist()}")

        # Not enough data to score: either no rows or the participant key is
        # missing from the feature matrix.
        if df.empty or 'participant_id' not in df.columns:
            raise AirflowSkipException(
                "Feature data is empty or missing 'participant_id'; nothing to predict."
            )

        df['prediction'] = df['participant_id'].apply(lambda x: random.choice([0, 1]))
        logger.info(f"Prediction results: {df[['participant_id', 'prediction']]}")
        return df[['participant_id', 'prediction']].to_dict(orient='records')