""" Testing Rebalancing """
import unittest
import os
from datetime import date

from indices import parse_daily_prices, LoaderDecimalCSV
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

    def test_load_prices(self):
        test_prices_path = 'tests/nyse-2018'
        for filename in sorted(os.listdir(test_prices_path)):
            if not filename.endswith('.csv'):
                continue

            prices_file = os.path.abspath(os.sep.join([test_prices_path, filename]))
            with open(prices_file, 'r') as prices:
                lines = prices.readlines()
                prices = parse_daily_prices(lines)

    def test_back_calculation(self):
        test_prices_path = 'tests/fake-data'
        for filename in sorted(os.listdir(test_prices_path)):
            if not (filename.endswith('.csv') and filename.startswith('US_2020')):
                continue

            prices_file = os.path.abspath(os.sep.join([test_prices_path, filename]))
            with open(prices_file, 'r') as prices:
                print(prices_file)
                lines = prices.readlines()
                loader = LoaderDecimalCSV(['Close', 'Volume'], '%d-%b-%Y')
                print(loader.load(lines))


if __name__ == '__main__':
    unittest.main()

