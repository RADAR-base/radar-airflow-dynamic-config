from dagloader.datareader.kafkadatareader import KafkaDataReader


class DataReaderFactory:
    @staticmethod
    def get_data_reader(reader_type: str, **kwargs) -> KafkaDataReader:
        if reader_type == 'kafka':
            return KafkaDataReader(
                conn_id=kwargs.get('conn_id', 'kafka_default'),
                topics=kwargs.get('topics', []),
                max_messages=kwargs.get('max_messages', 1000),
                poll_timeout=kwargs.get('poll_timeout', 5)
            )
        else:
            raise ValueError(f"Unsupported reader type: {reader_type}")
