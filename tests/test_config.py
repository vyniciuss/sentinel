from sentinel.models import Config
from sentinel.utils.utils import read_config_file


def test_load_config(file_path, spark):
    config = read_config_file(file_path, spark)
    assert isinstance(config, Config)
    assert config.sources_config is not None
    assert config.destinations_config is not None


def test_find_methods(file_path, spark):
    config = read_config_file(file_path, spark)

    column_mapping = config.find_column_mapping('tableA')
    assert column_mapping is not None
    assert column_mapping.name == 'tableA'

    query_mapping = config.find_query_mapping('input')
    assert query_mapping is not None
    assert query_mapping.name == 'input'

    source_config = config.find_source_config('tableA')
    assert source_config is not None
    assert source_config.name == 'tableA'

    destination_config = config.find_destination_config('transaction')
    assert destination_config is not None
    assert destination_config.name == 'transaction'
