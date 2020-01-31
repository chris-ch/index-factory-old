""" Testing Rebalancing """
import unittest
import os
from datetime import date

from indices import parse_daily_prices
from rebalancing import first_last_weekday, is_rebalancing_day


class TestRebalancing(unittest.TestCase):
    """
    Testing rebalancing.
    """

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_first_last_weekday(self):
        first, last = first_last_weekday(2020, 2)
        self.assertEqual(first, 3)
        self.assertEqual(last, 24)

    def test_is_rebalancing_day(self):
        self.assertTrue(is_rebalancing_day(date(2020, 2, 3)))
        self.assertFalse(is_rebalancing_day(date(2020, 2, 4)))

    def test_back_calculation(self):
        test_prices_path = 'tests/nyse-2018'
        for filename in sorted(os.listdir(test_prices_path)):
            if not filename.endswith('.csv'):
                continue

            prices_file = os.path.abspath(os.sep.join([test_prices_path, filename]))
            with open(prices_file, 'r') as prices:
                prices = parse_daily_prices(prices.readlines())
                print(prices)


if __name__ == '__main__':
    unittest.main()

