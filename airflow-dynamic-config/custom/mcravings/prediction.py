import random
import os
import sys
from pathlib import Path

_SRC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "src")
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

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
        # if multiple true predictions exist for a participant, keep the latest window's prediction only
        prediction_df = df.loc[df['prediction'] == 1]
        prediction_df = prediction_df.loc[prediction_df.groupby('participant_id')['window_start'].idxmax()]
        logger.info(f"Predicted craving for participants {prediction_df['participant_id'].values} for timestamps {prediction_df['window_start'].values}")
        prediction_df = prediction_df.loc[prediction_df['window_start'] > pd.Timestamp.now(tz='UTC') - pd.Timedelta(minutes=30)]
        return prediction_df[['participant_id', 'prediction', 'window_start']].to_dict(orient='records')