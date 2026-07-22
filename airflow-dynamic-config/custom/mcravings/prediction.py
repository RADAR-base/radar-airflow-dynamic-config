import random
from pathlib import Path
import pandas as pd
import logging
from airflow.exceptions import AirflowSkipException
import pickle
logger = logging.getLogger(__name__)

class PredictionTaskProcessor():
    def __init__(self):
        self.MODEL_DIR = Path(__file__).resolve().parent / 'models'
        super().__init__()

    def execute(self, data):
        dfs = []
        for key, value in data.items():
            dfs.append(pd.DataFrame(value))
        df = pd.concat(dfs, ignore_index=True)
        logger.info(f"Feature columns: {df.columns.tolist()}")
        if not df.shape[0]:
            raise AirflowSkipException("No feature data to predict on.")
        if df.empty or 'participant_id' not in df.columns:
            raise AirflowSkipException(
                "Feature data is empty or missing 'participant_id'; nothing to predict."
            )
        with open(self.MODEL_DIR / 'model_pipeline.pickle', 'rb') as f :
            loaded_model = pickle.load(f)
        df['prediction_prob'] = loaded_model.predict_proba(df[loaded_model.feature_names_in_])[:, 1]
        df['prediction'] = (df['prediction_prob'] > 0.5).astype(int)
        logger.info(df)
        return df[['participant_id', 'prediction', 'window_start']].to_dict(orient='records')