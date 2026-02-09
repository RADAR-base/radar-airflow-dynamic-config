from dagloader.datareader.datareader import DataReader
import pandas as pd

class LocalCSVDataReader(DataReader):
    def __init__(self, file_paths: list):
        self.file_paths = file_paths

    def get_reader_task(self):
        # For simplicity, we will read all CSV files and concatenate them into a single DataFrame
        dataframes = []
        for file_path in self.file_paths:
            df = pd.read_csv(file_path)
            dataframes.append(df)
        combined_df = pd.concat(dataframes, ignore_index=True)
        return combined_df