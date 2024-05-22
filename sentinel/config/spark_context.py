# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession


def get_spark_context():
    return SparkSession.builder.getOrCreate()
