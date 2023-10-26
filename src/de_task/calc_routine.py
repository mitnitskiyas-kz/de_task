"""Module for used routine."""
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import date_add, trunc, col, to_timestamp, when


def get_all_months_within(date_from: str, date_to: str):
    """Return all months between two dates.

    :param date_from: start date of the period
    :param date_to: end date of period
    :return: array of month like  ['Dec-2017', 'Jan-2018', 'Feb-2018']
    are within 2017-12-30 and 2018-02-01
    """
    d_date_from = datetime.strptime(date_from, '%Y-%m-%d')
    d_date_to = datetime.strptime(date_to, '%Y-%m-%d')
    month_list = \
        [datetime.strptime('%2.2d-%2.2d' % (year, month),
                           '%Y-%m').strftime('%b-%Y')
         for year in range(d_date_from.year,
                           d_date_to.year + 1)
         for month in range(d_date_from.month
                            if year == d_date_from.year else 1,
                            d_date_to.month + 1
                            if year == d_date_to.year else 13)]
    return month_list


def align_to_weekends(input_dataframe: DataFrame,
                      timestamp_column: str, new_column: str) -> DataFrame:
    """Align dataframe timestamp_column to end-of-week new_column."""
    return input_dataframe.withColumn(new_column, trunc(timestamp_column, 'week')). \
        withColumn(new_column,
                   when(to_timestamp(timestamp_column) == to_timestamp(new_column),
                        col(new_column)).otherwise(date_add(new_column, 7)))
