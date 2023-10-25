"""Module tests all routines."""
# import pytest
# from src.test.spark_base import spark
from src.main.python.calc_rouitine import get_all_months_within


class TestMe:
    """Class tests all routines."""

    def test_get_all_months_within(self):
        """Method tests function get_all_month_within."""
        test_date_from = '2016-12-01'
        test_date_to = '2017-04-01'
        result = get_all_months_within(test_date_from, test_date_to)
        assert result == ['Dec-2016', 'Jan-2017', 'Feb-2017',
                          'Mar-2017', 'Apr-2017']
