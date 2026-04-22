import yaml
from typing import Any, Dict
#from schema import Schema, SchemaError


class ConfigLoader:
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config = self.load_config()
        self.verify_config()

    def load_config(self) -> Dict:
        """Load and parse a YAML configuration file."""
        with open(self.config_path, 'r') as file:
            return yaml.safe_load(file)

    def verify_config(self):
        # TODO: Implement schema verification logic here
        return

    def get_config(self) -> Dict:
        return self.config
