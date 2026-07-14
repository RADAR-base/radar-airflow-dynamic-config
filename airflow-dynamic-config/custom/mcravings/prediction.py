import random
import pandas as pd
import logging
from airflow.exceptions import AirflowSkipException
logger = logging.getLogger(__name__)
class PredictionTaskProcessor():
    def __init__(self):
        super().__init__()

    def execute(self, data):
        dfs = []
        for key, value in data.items():
            dfs.append(pd.DataFrame(value))
        df = pd.concat(dfs, ignore_index=True)
        if not df.shape[0]:
            raise AirflowSkipException("No feature data to predict on.")
        if df.empty or 'participant_id' not in df.columns:
            raise AirflowSkipException(
                "Feature data is empty or missing 'participant_id'; nothing to predict."
            )
        df['prediction'] = df['participant_id'].apply(lambda x: random.choice([0, 1]))
        df = df.groupby('participant_id')['prediction'].mean().reset_index()
        df['prediction'] = df['prediction'].apply(lambda x: 1 if x >= 0.5 else 0)
        logger.info(df)
        return df[['participant_id', 'prediction']].to_dict(orient='records')