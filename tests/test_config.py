import os

import pytest

from sentinel.models import Config
from sentinel.utils.utils import load_config


@pytest.fixture
def file_path():
    return os.path.join(os.path.dirname(__file__), 'resources', 'process.json')


def test_load_config(file_path, spark):
    config = load_config(file_path, spark)
    assert isinstance(config, Config)
    assert config.flow_type == 'onetoone'
    assert config.sources_config is not None
    assert config.destinations_config is not None


def test_find_methods(file_path, spark):
    config = load_config(file_path)

    column_mapping = config.find_column_mapping('merchant')
    assert column_mapping is not None
    assert column_mapping.name == 'merchant'

    query_mapping = config.find_query_mapping('input')
    assert query_mapping is not None
    assert query_mapping.name == 'input'

    source_config = config.find_source_config('merchant')
    assert source_config is not None
    assert source_config.name == 'merchant'

    destination_config = config.find_destination_config('transaction')
    assert destination_config is not None
    assert destination_config.name == 'transaction'


def test_yaml_path(file_path, spark):
    config = load_config(file_path)
    dest_config = config.find_destination_config('transaction')
    assert dest_config is not None
    assert (
        dest_config.yaml_path
        == 'D:\\Estudos\\sentinel\\tests\\resources\\datacontract.yaml'
    )
