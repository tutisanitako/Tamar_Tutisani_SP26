"""
Single place that knows how to find and read a YAML config file under
Configs/. Every conftest.py (root, unit, integration, ui) imports this
instead of duplicating the "find the project root, open the yaml" logic
that used to live inside Tests/login.py's get_selenium_config().
"""
import os
import yaml


def load_config(config_file_name: str) -> dict:
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(project_root, "Configs", config_file_name)
    with open(config_path, "r") as stream:
        return yaml.safe_load(stream)
