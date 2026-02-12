from airflow.sdk import BaseOperator
import dagloader.datareader.datareaderfactory as DataReaderFactory


class DataReaderOperator(BaseOperator):
    def __init__(self, data_config: dict, source_config: dict,
                 intermediate_storage, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data_config = data_config
        self.source_config = source_config
        self.intermediate_storage = intermediate_storage
        self.source_name = source_config.get('name')
        self.source_type = source_config.get('type')

    def execute(self, context):
        reader_config = self.data_config.get('data', {})
        reader = DataReaderFactory.get_data_reader(self.source_type,
                                                   **reader_config)
        data = reader.read_data()
        self.intermediate_storage.save(self.source_name, data)
