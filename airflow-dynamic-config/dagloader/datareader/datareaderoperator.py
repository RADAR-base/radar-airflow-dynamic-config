from airflow.sdk import BaseOperator
from dagloader.datareader.datareaderfactory import DataReaderFactory


class DataReaderOperator(BaseOperator):
    def __init__(self, data_config: dict, source_config: dict,
                 intermediate_storage, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data_config = data_config
        self.source_config = source_config
        self.intermediate_storage = intermediate_storage
        self.source_name = source_config.get('name')
        self.source_type = source_config.get('type')
        self.source_config = source_config.get('config', {})

    def execute(self, context):
        reader = DataReaderFactory.get_data_reader(self.source_type,
                                                   **self.source_config)
        data = reader.read_data()
        self.intermediate_storage.save(self.source_name, data)
