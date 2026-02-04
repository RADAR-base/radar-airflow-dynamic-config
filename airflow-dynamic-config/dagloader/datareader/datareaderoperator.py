from airflow.sdk import BaseOperator
import dagloader.datareader.datareaderfactory as DataReaderFactory


class DataReaderOperator(BaseOperator):
    def __init__(self, reader_config: dict, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.reader_config = reader_config

    def execute(self, context):
        reader_type = self.reader_config.get('type')
        reader = DataReaderFactory.get_data_reader(reader_type,
                                                   **self.reader_config)
        data = reader.read_data()
        return data
