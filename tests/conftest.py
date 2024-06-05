import logging
import os
import shutil
import uuid
from os import path

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

from sentinel.config.logging_config import logger
from sentinel.models import DataQualityConfig

BASE_TEMP_DIR = path.join(os.getcwd(), 'temp_test_dirs')
TEMPDIR = path.join(BASE_TEMP_DIR, f'spark_temp_dir_{uuid.uuid4()}')
METASTORE_DB = path.join(BASE_TEMP_DIR, 'metastore_db')
SPARK_WAREHOUSE = path.join(BASE_TEMP_DIR, 'spark-warehouse')
DERBY_LOG = path.join(BASE_TEMP_DIR, 'derby.log')
CHECKPOINT = path.join(BASE_TEMP_DIR, 'checkpoint')
TMP = path.join(BASE_TEMP_DIR, 'tmp')

config_spark = {
    'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
    'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog',
    'spark.databricks.delta.retentionDurationCheck.enabled': 'false',
    'spark.local.dir': TEMPDIR,
    'spark.sql.warehouse.dir': SPARK_WAREHOUSE,
    'spark.sql.session.timeZone': 'UTC',
    'spark.driver.memory': '2g',
    'spark.sql.shuffle.partitions': 1,
    'spark.default.parallelism': 1,
    'spark.ui.showConsoleProgress': True,
    'spark.ui.enabled': False,
}


def quiet_py4j(spark):
    """Diminui o logging do Py4J no contexto de teste"""
    logger_py4j = logging.getLogger('py4j')
    s_logger_py4j = logging.getLogger('py4j.java_gateway')
    logger_py4j.setLevel(logging.FATAL)
    s_logger_py4j.setLevel(logging.FATAL)
    spark.sparkContext.setLogLevel('ERROR')


@pytest.fixture(scope='session')
def spark():
    remove_temp_folders()
    os.makedirs(TEMPDIR, exist_ok=True)
    os.makedirs(SPARK_WAREHOUSE, exist_ok=True)
    os.makedirs(CHECKPOINT, exist_ok=True)
    os.makedirs(TMP, exist_ok=True)

    os.environ['SPARK_LOCAL_DIRS'] = TEMPDIR
    logger.info(f'Temporary directory created: {TEMPDIR}')

    spark_builder = SparkSession.builder.master('local[*]').appName(
        'PySpark Session'
    )

    for key, val in config_spark.items():
        spark_builder = spark_builder.config(key, val)

    spark = (
        configure_spark_with_delta_pip(spark_builder)
        .enableHiveSupport()
        .getOrCreate()
    )

    quiet_py4j(spark)
    yield spark

    spark.stop()


@pytest.fixture
def setup_data(spark):
    spark.sql('CREATE DATABASE IF NOT EXISTS test_db')
    spark.sql('USE test_db')

    data = [
        ('Alice', 30, 'single'),
        ('Alice', 30, 'married'),
        ('Bob', 45, 'married'),
        ('Charlie', 45, 'divorced'),
    ]
    columns = ['name', 'age', 'status']
    df = spark.createDataFrame(data, columns)
    df.write.format('delta').mode('overwrite').saveAsTable(
        'test_db.source_table'
    )

    result_table_path = os.path.join(
        SPARK_WAREHOUSE, 'test_db.db', 'result_table'
    )
    if os.path.exists(result_table_path):
        shutil.rmtree(result_table_path)

    try:
        spark.sql(
            'CREATE TABLE IF NOT EXISTS test_db.result_table ('
            'expectation_type STRING, kwargs STRING, success BOOLEAN, error_message STRING, '
            'observed_value STRING, severity STRING, table_name STRING, execution_date STRING, '
            'validation_id STRING, validation_time FLOAT, batch_id STRING, '
            'datasource_name STRING, dataset_name STRING, expectation_suite_name STRING) '
            'USING delta'
        )
    except AnalysisException:
        spark.sql('DROP TABLE IF EXISTS test_db.result_table')
        spark.sql(
            'CREATE TABLE test_db.result_table ('
            'expectation_type STRING, kwargs STRING, success BOOLEAN, error_message STRING, '
            'observed_value STRING, severity STRING, table_name STRING, execution_date STRING, '
            'validation_id STRING, validation_time FLOAT, batch_id STRING, '
            'datasource_name STRING, dataset_name STRING, expectation_suite_name STRING) '
            'USING delta'
        )

    yield

    spark.sql('DROP DATABASE IF EXISTS test_db CASCADE')


@pytest.fixture
def mock_config():
    return DataQualityConfig(
        great_expectations=[
            {
                'expectation_type': 'expect_column_to_exist',
                'kwargs': {'column': 'name'},
            },
            {
                'expectation_type': 'expect_column_values_to_not_be_null',
                'kwargs': {'column': 'age'},
            },
            {
                'expectation_type': 'expect_column_values_to_be_in_set',
                'kwargs': {
                    'column': 'status',
                    'value_set': ['single', 'married', 'divorced'],
                },
            },
        ],
        custom_expectations=[],
    )


@pytest.fixture
def file_path():
    return os.path.join(os.path.dirname(__file__), 'resources', 'process.json')


def remove_temp_folders():
    folders_to_remove = [
        TEMPDIR,
        SPARK_WAREHOUSE,
        DERBY_LOG,
        METASTORE_DB,
        CHECKPOINT,
        TMP,
    ]

    for folder in folders_to_remove:
        if os.path.exists(folder):
            try:
                if os.path.isdir(folder):
                    shutil.rmtree(folder)
                else:
                    os.remove(folder)
                logger.info(f'Removed temporary folder/file: {folder}')
            except Exception as e:
                logger.error(f'Error removing {folder}: {e}')


logging.basicConfig(level=logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())
