"""Module tests all routines."""
import pytest
from src.test.spark_base import spark
from de_task.calc_rouitine import get_all_months_within, align_to_weekends
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType


class TestMe:
    """Class tests all routines."""

    def test_get_all_months_within(self):
        """Method tests function get_all_month_within."""
        test_date_from = '2016-12-01'
        test_date_to = '2017-04-01'
        result = get_all_months_within(test_date_from, test_date_to)
        assert result == ['Dec-2016', 'Jan-2017', 'Feb-2017',
                          'Mar-2017', 'Apr-2017']

    def test_align_to_weekends(self, spark: SparkSession):
        """Method tests aligning to weekends."""
        input_data: list = [{'original_timestamp': '2023-10-08 23:59:59'},
                            {'original_timestamp': '2023-10-09 00:00:00'},
                            {'original_timestamp': '2023-10-09 00:00:01'}]
        input_dataframe = spark.createDataFrame(input_data).coalesce(1)
        output_dataframe = align_to_weekends(input_dataframe,
                                             'original_timestamp',
                                             'aligned_data') \
            .withColumn('aligned_data', col('aligned_data').cast(StringType()))
        output_data_calculated = [row.asDict() for row in output_dataframe.collect()]
        output_data_correct = [{'original_timestamp': '2023-10-08 23:59:59',
                                'aligned_data': '2023-10-09'},
                               {'original_timestamp': '2023-10-09 00:00:00',
                                'aligned_data': '2023-10-09'},
                               {'original_timestamp': '2023-10-09 00:00:01',
                                'aligned_data': '2023-10-16'}]
        assert output_data_calculated == output_data_correct
