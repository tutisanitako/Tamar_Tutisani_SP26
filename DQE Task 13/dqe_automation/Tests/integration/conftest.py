import pytest
from Utils.config_reader import load_config


@pytest.fixture(scope="session")
def api_config():
    return load_config("config_api_integration.yaml")
