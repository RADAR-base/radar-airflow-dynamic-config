from dagloader.watchers.kafkawatcher import KafkaWatcher


class WatcherFactory:
    """Build event-driven watchers from a config ``type``.

    A watcher produces an Airflow ``Asset`` whose ``AssetWatcher`` trigger wakes
    a DAG when matching upstream data appears. Register new watcher types in
    ``_WATCHERS`` to extend the framework without touching ``DAGMaker``.
    """

    _WATCHERS = {
        'kafka': KafkaWatcher,
    }

    @classmethod
    def get_watcher(cls, watcher_type: str, name: str, config: dict):
        watcher_cls = cls._WATCHERS.get(watcher_type)
        if watcher_cls is None:
            raise ValueError(f"Unsupported watcher type: {watcher_type}")
        return watcher_cls(name=name, config=config)
