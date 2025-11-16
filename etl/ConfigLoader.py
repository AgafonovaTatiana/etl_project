import logging
import json

class ConfigLoader:
    def __init__(self, config_path):
        self.config_path = config_path
        self.config = self.load_config()
        
    def load_config(self):
        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)
            return config
        except FileNotFoundError:
            logging.error(f"Configuration file not found")
            return None
        except Exception as e:
            logging.error(f"An unexpected error occurred loading config: {e}")
            return None