"""Module defines base object for testing process."""
import pytest
from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    """Return SparkSession object."""
    spark = SparkSession.builder \
        .master('local[1]') \
        .appName('sparkTesting') \
        .config('spark.executor.cores', '1') \
        .config('spark.executor.instances', '1') \
        .config('spark.sql.shuffle.partitions', '1') \
        .getOrCreate()
    return spark


@pytest.fixture()
def spark():
    """Return the spark object.

    :return:
    """
    print('spark setup')
    spark_session = get_spark()
    yield spark_session
    print('teardown')
