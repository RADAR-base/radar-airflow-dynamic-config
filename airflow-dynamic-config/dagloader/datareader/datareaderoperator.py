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
        # Update source_config from runtime parameters if available
        runtime_data_configs = context['params'].get('data', {})
        runtime_source_types = runtime_data_configs.get('source_types', []) if isinstance(runtime_data_configs, dict) else context['params'].get('source_types', [])
        runtime_source_config = next((s for s in runtime_source_types if s.get('name') == self.source_name), self.source_config)

        # Merge with existing
        self.source_config = runtime_source_config.get('config', runtime_source_config)
        self.source_type = runtime_source_config.get('type', self.source_type)

        reader = DataReaderFactory.get_data_reader(self.source_type,
                                                   **self.source_config)
        data = reader.read_data()
        self.intermediate_storage.save(self.source_name, data)
