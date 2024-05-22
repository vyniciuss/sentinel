import logging
import os
import shutil
import uuid
from os import path

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from sentinel.config.logging_config import (
    logger,
)

TEMPDIR = path.join(os.getcwd(), 'tmp', f'spark_temp_dir_{uuid.uuid4()}')

config_spark = {
    'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
    'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog',
    'spark.databricks.delta.retentionDurationCheck.enabled': 'false',
    'spark.local.dir': TEMPDIR,
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
    spark.sparkContext.setLogLevel('fatal')


@pytest.fixture(scope='session')
def spark():

    if os.path.exists(TEMPDIR):
        try:
            shutil.rmtree(TEMPDIR, ignore_errors=True)
        except Exception as e:
            logger.error(
                f'Error removing existing temporary directory at start: {e}'
            )
    os.makedirs(TEMPDIR, exist_ok=True)
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

    if os.path.exists(TEMPDIR):
        logger.info(f'Removing temporary directory: {TEMPDIR}')
        try:
            shutil.rmtree(TEMPDIR)
        except Exception as e:
            logger.error(f'Error removing temporary directory: {e}')
    else:
        logger.warning(f'Temporary directory not found: {TEMPDIR}')


logging.basicConfig(level=logging.INFO)
logging.getLogger().addHandler(logger)
