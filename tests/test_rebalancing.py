""" Testing Rebalancing """
import unittest
import os
from datetime import date

from indices import parse_daily_prices, LoaderDecimalCSV
from rebalancing import first_last_weekday_month, is_rebalancing_day, RebalancingRule, RebalancingFrequency, WeekDay, RebalancingDay


class TestRebalancing(unittest.TestCase):
    """
    Testing rebalancing
    """

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_first_last_weekday(self):
        first, last = first_last_weekday_month(2020, 2)
        self.assertEqual(first, 3)
        self.assertEqual(last, 24)

    def test_is_rebalancing_day(self):
        self.assertFalse(is_rebalancing_day(date(2020, 2, 3), RebalancingRule(RebalancingFrequency.MONTHLY, WeekDay.TUESDAY, RebalancingDay.LAST_DAY_OF_PERIOD)))
        self.assertFalse(is_rebalancing_day(date(2020, 2, 4), RebalancingRule(RebalancingFrequency.MONTHLY, WeekDay.TUESDAY, RebalancingDay.LAST_DAY_OF_PERIOD)))
        self.assertFalse(is_rebalancing_day(date(2020, 2, 24), RebalancingRule(RebalancingFrequency.MONTHLY, WeekDay.TUESDAY, RebalancingDay.LAST_DAY_OF_PERIOD)))
        self.assertTrue(is_rebalancing_day(date(2020, 2, 25), RebalancingRule(RebalancingFrequency.MONTHLY, WeekDay.TUESDAY, RebalancingDay.LAST_DAY_OF_PERIOD)))
        self.assertFalse(is_rebalancing_day(date(2020, 2, 26), RebalancingRule(RebalancingFrequency.MONTHLY, WeekDay.TUESDAY, RebalancingDay.LAST_DAY_OF_PERIOD)))

        self.assertFalse(is_rebalancing_day(date(2020, 2, 3), RebalancingRule(RebalancingFrequency.MONTHLY, WeekDay.TUESDAY, RebalancingDay.FIRST_DAY_OF_PERIOD)))
        self.assertTrue(is_rebalancing_day(date(2020, 2, 4), RebalancingRule(RebalancingFrequency.MONTHLY, WeekDay.TUESDAY, RebalancingDay.FIRST_DAY_OF_PERIOD)))
        self.assertFalse(is_rebalancing_day(date(2020, 2, 24), RebalancingRule(RebalancingFrequency.MONTHLY, WeekDay.TUESDAY, RebalancingDay.FIRST_DAY_OF_PERIOD)))
        self.assertFalse(is_rebalancing_day(date(2020, 2, 25), RebalancingRule(RebalancingFrequency.MONTHLY, WeekDay.TUESDAY, RebalancingDay.FIRST_DAY_OF_PERIOD)))
        self.assertFalse(is_rebalancing_day(date(2020, 2, 26), RebalancingRule(RebalancingFrequency.MONTHLY, WeekDay.TUESDAY, RebalancingDay.FIRST_DAY_OF_PERIOD)))

        self.assertFalse(is_rebalancing_day(date(2020, 2, 3), RebalancingRule(RebalancingFrequency.QUARTERLY, WeekDay.TUESDAY, RebalancingDay.FIRST_DAY_OF_PERIOD)))
        self.assertFalse(is_rebalancing_day(date(2020, 2, 4), RebalancingRule(RebalancingFrequency.QUARTERLY, WeekDay.TUESDAY, RebalancingDay.FIRST_DAY_OF_PERIOD)))
        self.assertFalse(is_rebalancing_day(date(2020, 2, 24), RebalancingRule(RebalancingFrequency.QUARTERLY, WeekDay.TUESDAY, RebalancingDay.FIRST_DAY_OF_PERIOD)))
        self.assertFalse(is_rebalancing_day(date(2020, 2, 25), RebalancingRule(RebalancingFrequency.QUARTERLY, WeekDay.TUESDAY, RebalancingDay.FIRST_DAY_OF_PERIOD)))
        self.assertFalse(is_rebalancing_day(date(2020, 2, 26), RebalancingRule(RebalancingFrequency.QUARTERLY, WeekDay.TUESDAY, RebalancingDay.FIRST_DAY_OF_PERIOD)))

        self.assertFalse(is_rebalancing_day(date(2020, 4, 6), RebalancingRule(RebalancingFrequency.QUARTERLY, WeekDay.TUESDAY, RebalancingDay.FIRST_DAY_OF_PERIOD)))
        self.assertTrue(is_rebalancing_day(date(2020, 4, 7), RebalancingRule(RebalancingFrequency.QUARTERLY, WeekDay.TUESDAY, RebalancingDay.FIRST_DAY_OF_PERIOD)))
        self.assertFalse(is_rebalancing_day(date(2020, 4, 28), RebalancingRule(RebalancingFrequency.QUARTERLY, WeekDay.TUESDAY, RebalancingDay.FIRST_DAY_OF_PERIOD)))

        self.assertFalse(is_rebalancing_day(date(2020, 6, 2), RebalancingRule(RebalancingFrequency.QUARTERLY, WeekDay.TUESDAY, RebalancingDay.LAST_DAY_OF_PERIOD)))
        self.assertTrue(is_rebalancing_day(date(2020, 6, 30), RebalancingRule(RebalancingFrequency.QUARTERLY, WeekDay.TUESDAY, RebalancingDay.LAST_DAY_OF_PERIOD)))

    def test_load_prices(self):
        test_prices_path = 'resources/nyse-2018'
        for filename in sorted(os.listdir(test_prices_path)):
            if not filename.endswith('.csv'):
                continue

            prices_file = os.path.abspath(os.sep.join([test_prices_path, filename]))
            with open(prices_file, 'r') as prices:
                lines = prices.readlines()
                _ = parse_daily_prices(lines)

    def test_back_calculation(self):
        test_prices_path = 'resources/fake-data'
        dates = list()
        counts = list()
        for filename in sorted(os.listdir(test_prices_path)):
            if not (filename.endswith('.csv') and filename.startswith('US_2020')):
                continue

            prices_file = os.path.abspath(os.sep.join([test_prices_path, filename]))
            with open(prices_file, 'r') as prices:
                lines = prices.readlines()
                loader = LoaderDecimalCSV(['Close', 'Volume'], '%d-%b-%Y')
                as_of_date, results = loader.load(lines)
                dates.append(as_of_date)
                counts.append(len(results['Close']))
        
        self.assertSequenceEqual(dates, [date(2020, 1, 31), date(2020, 2, 3),
            date(2020, 2, 28), date(2020, 3, 2),
            date(2020, 3, 31), date(2020, 4, 6)])
        self.assertSequenceEqual(counts, [5, 5, 5, 5, 4, 4])


if __name__ == '__main__':
    unittest.main()

