""" Testing Rebalancing """
import unittest
import os
from datetime import date
from enum import Enum

from indices import parse_daily_prices, LoaderDecimalCSV
from rebalancing import first_last_weekday_month, first_last_weekday_quarter, is_rebalancing_day, get_rebalancing_day_next, get_rebalancing_day_previous
from rebalancing import RebalancingRule, RebalancingFrequency, WeekDay, RebalancingSide


class TestRebalancing(unittest.TestCase):
    """
    Testing rebalancing
    """

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_weekdays(self):
        self.assertEqual(0, WeekDay.MONDAY.position)
        self.assertEqual(1, WeekDay.TUESDAY.position)
        self.assertEqual(2, WeekDay.WEDNESDAY.position)
        self.assertEqual(3, WeekDay.THURSDAY.position)
        self.assertEqual(4, WeekDay.FRIDAY.position)
        self.assertEqual(5, WeekDay.SATURDAY.position)
        self.assertEqual(6, WeekDay.SUNDAY.position)

    def test_first_last_weekday(self):
        first, last = first_last_weekday_month(2020, 2)
        self.assertEqual(first, date(2020, 2, 3))
        self.assertEqual(last, date(2020, 2, 24))

    def test_first_last_weekday_quarter(self):
        first, last = first_last_weekday_quarter(date(2020, 11, 12))
        self.assertEqual(first, date(2020, 10, 5))
        self.assertEqual(last, date(2020, 12, 28))
        first, last = first_last_weekday_quarter(date(2020, 3, 12))
        self.assertEqual(first, date(2020, 1, 6))
        self.assertEqual(last, date(2020, 3, 30))

    def test_get_rebalancing_day(self):
        monthly_last_tuesday = RebalancingRule(RebalancingFrequency.MONTHLY, WeekDay.TUESDAY, RebalancingSide.LAST_DAY_OF_PERIOD)
        monthly_first_tuesday = RebalancingRule(RebalancingFrequency.MONTHLY, WeekDay.TUESDAY, RebalancingSide.FIRST_DAY_OF_PERIOD)
        self.assertEqual(date(2020, 2, 25), get_rebalancing_day_next(date(2020, 2, 3), monthly_last_tuesday))
        self.assertEqual(date(2020, 2, 25), get_rebalancing_day_next(date(2020, 2, 4), monthly_last_tuesday))
        self.assertEqual(date(2020, 2, 25), get_rebalancing_day_next(date(2020, 2, 24), monthly_last_tuesday))
        self.assertEqual(date(2020, 2, 25), get_rebalancing_day_next(date(2020, 2, 25), monthly_last_tuesday))
        self.assertEqual(date(2020, 3, 31), get_rebalancing_day_next(date(2020, 2, 26), monthly_last_tuesday))

        self.assertEqual(date(2020, 2, 4), get_rebalancing_day_next(date(2020, 1, 28), monthly_first_tuesday))
        self.assertEqual(date(2020, 2, 4), get_rebalancing_day_next(date(2020, 2, 3), monthly_first_tuesday))
        self.assertEqual(date(2020, 2, 4), get_rebalancing_day_next(date(2020, 2, 4), monthly_first_tuesday))
        self.assertEqual(date(2020, 3, 3), get_rebalancing_day_next(date(2020, 2, 5), monthly_first_tuesday))
        self.assertEqual(date(2020, 3, 3), get_rebalancing_day_next(date(2020, 2, 24), monthly_first_tuesday))
        self.assertEqual(date(2020, 3, 3), get_rebalancing_day_next(date(2020, 2, 25), monthly_first_tuesday))
        self.assertEqual(date(2020, 3, 3), get_rebalancing_day_next(date(2020, 2, 26), monthly_first_tuesday))

        self.assertEqual(date(2020, 4, 7), get_rebalancing_day_next(date(2020, 2, 3), RebalancingRule(RebalancingFrequency.QUARTERLY, WeekDay.TUESDAY, RebalancingSide.FIRST_DAY_OF_PERIOD)))
        self.assertEqual(date(2020, 4, 7), get_rebalancing_day_next(date(2020, 2, 4), RebalancingRule(RebalancingFrequency.QUARTERLY, WeekDay.TUESDAY, RebalancingSide.FIRST_DAY_OF_PERIOD)))
        self.assertEqual(date(2020, 4, 7), get_rebalancing_day_next(date(2020, 2, 24), RebalancingRule(RebalancingFrequency.QUARTERLY, WeekDay.TUESDAY, RebalancingSide.FIRST_DAY_OF_PERIOD)))
        self.assertEqual(date(2020, 4, 7), get_rebalancing_day_next(date(2020, 2, 25), RebalancingRule(RebalancingFrequency.QUARTERLY, WeekDay.TUESDAY, RebalancingSide.FIRST_DAY_OF_PERIOD)))
        self.assertEqual(date(2020, 4, 7), get_rebalancing_day_next(date(2020, 2, 26), RebalancingRule(RebalancingFrequency.QUARTERLY, WeekDay.TUESDAY, RebalancingSide.FIRST_DAY_OF_PERIOD)))

        self.assertEqual(date(2020, 4, 7), get_rebalancing_day_next(date(2020, 4, 6), RebalancingRule(RebalancingFrequency.QUARTERLY, WeekDay.TUESDAY, RebalancingSide.FIRST_DAY_OF_PERIOD)))
        self.assertEqual(date(2020, 4, 7), get_rebalancing_day_next(date(2020, 4, 7), RebalancingRule(RebalancingFrequency.QUARTERLY, WeekDay.TUESDAY, RebalancingSide.FIRST_DAY_OF_PERIOD)))
        self.assertEqual(date(2020, 7, 7), get_rebalancing_day_next(date(2020, 4, 28), RebalancingRule(RebalancingFrequency.QUARTERLY, WeekDay.TUESDAY, RebalancingSide.FIRST_DAY_OF_PERIOD)))

        self.assertEqual(date(2020, 6, 30), get_rebalancing_day_next(date(2020, 6, 2), RebalancingRule(RebalancingFrequency.QUARTERLY, WeekDay.TUESDAY, RebalancingSide.LAST_DAY_OF_PERIOD)))
        self.assertEqual(date(2020, 6, 30), get_rebalancing_day_next(date(2020, 6, 30), RebalancingRule(RebalancingFrequency.QUARTERLY, WeekDay.TUESDAY, RebalancingSide.LAST_DAY_OF_PERIOD)))
        
        # prev
        self.assertEqual(date(2020, 1, 28), get_rebalancing_day_previous(date(2020, 2, 3), monthly_last_tuesday))
        self.assertEqual(date(2020, 1, 28), get_rebalancing_day_previous(date(2020, 2, 4), monthly_last_tuesday))
        self.assertEqual(date(2020, 1, 28), get_rebalancing_day_previous(date(2020, 2, 24), monthly_last_tuesday))
        self.assertEqual(date(2020, 2, 25), get_rebalancing_day_previous(date(2020, 2, 25), monthly_last_tuesday))
        self.assertEqual(date(2020, 2, 25), get_rebalancing_day_previous(date(2020, 2, 26), monthly_last_tuesday))

    def test_is_rebalancing_day(self):
        monthly_last_tuesday = RebalancingRule(RebalancingFrequency.MONTHLY, WeekDay.TUESDAY, RebalancingSide.LAST_DAY_OF_PERIOD)
        monthly_first_tuesday = RebalancingRule(RebalancingFrequency.MONTHLY, WeekDay.TUESDAY, RebalancingSide.FIRST_DAY_OF_PERIOD)
        self.assertFalse(is_rebalancing_day(date(2020, 2, 3), monthly_last_tuesday))
        self.assertFalse(is_rebalancing_day(date(2020, 2, 4), monthly_last_tuesday))
        self.assertFalse(is_rebalancing_day(date(2020, 2, 24), monthly_last_tuesday))
        self.assertTrue(is_rebalancing_day(date(2020, 2, 25), monthly_last_tuesday))
        self.assertFalse(is_rebalancing_day(date(2020, 2, 26), monthly_last_tuesday))

        self.assertFalse(is_rebalancing_day(date(2020, 2, 3), monthly_first_tuesday))
        self.assertTrue(is_rebalancing_day(date(2020, 2, 4), monthly_first_tuesday))
        self.assertFalse(is_rebalancing_day(date(2020, 2, 24), monthly_first_tuesday))
        self.assertFalse(is_rebalancing_day(date(2020, 2, 25), monthly_first_tuesday))
        self.assertFalse(is_rebalancing_day(date(2020, 2, 26), monthly_first_tuesday))

        self.assertFalse(is_rebalancing_day(date(2020, 2, 3), RebalancingRule(RebalancingFrequency.QUARTERLY, WeekDay.TUESDAY, RebalancingSide.FIRST_DAY_OF_PERIOD)))
        self.assertFalse(is_rebalancing_day(date(2020, 2, 4), RebalancingRule(RebalancingFrequency.QUARTERLY, WeekDay.TUESDAY, RebalancingSide.FIRST_DAY_OF_PERIOD)))
        self.assertFalse(is_rebalancing_day(date(2020, 2, 24), RebalancingRule(RebalancingFrequency.QUARTERLY, WeekDay.TUESDAY, RebalancingSide.FIRST_DAY_OF_PERIOD)))
        self.assertFalse(is_rebalancing_day(date(2020, 2, 25), RebalancingRule(RebalancingFrequency.QUARTERLY, WeekDay.TUESDAY, RebalancingSide.FIRST_DAY_OF_PERIOD)))
        self.assertFalse(is_rebalancing_day(date(2020, 2, 26), RebalancingRule(RebalancingFrequency.QUARTERLY, WeekDay.TUESDAY, RebalancingSide.FIRST_DAY_OF_PERIOD)))

        self.assertFalse(is_rebalancing_day(date(2020, 4, 6), RebalancingRule(RebalancingFrequency.QUARTERLY, WeekDay.TUESDAY, RebalancingSide.FIRST_DAY_OF_PERIOD)))
        self.assertTrue(is_rebalancing_day(date(2020, 4, 7), RebalancingRule(RebalancingFrequency.QUARTERLY, WeekDay.TUESDAY, RebalancingSide.FIRST_DAY_OF_PERIOD)))
        self.assertFalse(is_rebalancing_day(date(2020, 4, 28), RebalancingRule(RebalancingFrequency.QUARTERLY, WeekDay.TUESDAY, RebalancingSide.FIRST_DAY_OF_PERIOD)))

        self.assertFalse(is_rebalancing_day(date(2020, 6, 2), RebalancingRule(RebalancingFrequency.QUARTERLY, WeekDay.TUESDAY, RebalancingSide.LAST_DAY_OF_PERIOD)))
        self.assertTrue(is_rebalancing_day(date(2020, 6, 30), RebalancingRule(RebalancingFrequency.QUARTERLY, WeekDay.TUESDAY, RebalancingSide.LAST_DAY_OF_PERIOD)))

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

