import random
import pandas as pd
import logging
logger = logging.getLogger(__name__)
class PredictionTaskProcessor():
    def __init__(self):
        super().__init__()

    def execute(self, data):
        dfs = []
        for data_sources in data.values():
            if isinstance(data_sources, list):
                dfs.append(pd.DataFrame(data_sources))
                continue
            for _, topic_data in data_sources.items():
                dfs.append(pd.DataFrame(topic_data))
        df = pd.concat(dfs, ignore_index=True)
        df['prediction'] = df['enrollment.participantID'].apply(lambda x: random.choice([0, 1]))
        return df[['enrollment.participantID', 'prediction']].to_dict(orient='records')