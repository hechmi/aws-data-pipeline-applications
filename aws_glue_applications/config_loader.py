import yaml
import os
from typing import Dict

def load_config(config_file: str = "default-config.yaml") -> Dict:
    """Load configuration from YAML file"""
    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), config_file)
    
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    
    return config
