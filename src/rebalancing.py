"""
Rebalancing.
"""
import calendar
from datetime import date
from enum import Enum
from typing import Iterable, Dict, Tuple
from decimal import Decimal


class Quarter(Enum):
    """
    Quarters.
    """
    Q1 = 0
    Q2 = 1
    Q3 = 2
    Q4 = 3


class WeekDay(Enum):
    """
    Enum version of calendar week days.
    """
    MONDAY = 0
    TUESDAY = 1
    WEDNESDAY = 2
    THURSDAY = 3
    FRIDAY = 4
    SATURDAY = 5
    SUNDAY = 6


class RebalancingFrequency(Enum):
    """
    Available rebalancing frequencies.
    """
    MONTHLY = 'monthly'
    QUARTERLY = 'quarterly'
    ANNUALLY = 'annually'


class RebalancingDay(Enum):
    LAST_DAY_OF_PERIOD = 'last day of period'
    FIRST_DAY_OF_PERIOD = 'first day of period'


class RebalancingRule(object):

    def __init__(self, rule_frequency: RebalancingFrequency, rule_week_day: WeekDay, rule_start_end: RebalancingDay):
        self._rule_frequency = rule_frequency
        self._rule_week_day = rule_week_day
        self._rule_start_end = rule_start_end

    @property
    def rule_frequency(self):
        return self._rule_frequency

    @property
    def rule_week_day(self):
        return self._rule_week_day

    @property
    def rule_start_end(self):
        return self._rule_start_end


def first_last_weekday_month(year: int, month: int, weekday: WeekDay = WeekDay.MONDAY) -> Tuple[date, date]:
    """ Checking rebalancing day.
    """
    weeks = calendar.monthcalendar(year, month)
    first_weekday = 0
    for week in weeks:
        if week[weekday.value] != 0:
            first_weekday = week[weekday.value]
            break

    last_weekday = 0
    for week in reversed(weeks):
        if week[weekday.value] != 0:
            last_weekday = week[weekday.value]
            break

    return date(year, month, first_weekday), date(year, month, last_weekday)


def first_last_weekday_quarter(as_of_date: date, weekday: WeekDay = WeekDay.MONDAY) -> Tuple[date, date]:
    """ Checking rebalancing day.
    """
    month_start = (as_of_date.month - 1) // 3 * 3 + 1
    month_end = month_start + 2
    quarter_start_weeks = calendar.monthcalendar(as_of_date.year, month_start)
    quarter_end_weeks = calendar.monthcalendar(as_of_date.year, month_end)
    first_weekday = 0
    for week in quarter_start_weeks:
        if week[weekday.value] != 0:
            first_weekday = week[weekday.value]
            break

    last_weekday = 0
    for week in reversed(quarter_end_weeks):
        if week[weekday.value] != 0:
            last_weekday = week[weekday.value]
            break

    return date(as_of_date.year, month_start, first_weekday), date(as_of_date.year, month_end, last_weekday)


def get_rebalancing_day_previous(as_of_date: date, rule: RebalancingRule) -> date:
    if rule.rule_frequency == RebalancingFrequency.MONTHLY:
        first_day, last_day = first_last_weekday_month(as_of_date.year, as_of_date.month, rule.rule_week_day)

    elif rule.rule_frequency == RebalancingFrequency.QUARTERLY:
        first_day, last_day = first_last_weekday_quarter(as_of_date, rule.rule_week_day)

    else:
        first_day, last_day = first_last_weekday_quarter(as_of_date, rule.rule_week_day)

    rebalancing_day = (first_day, last_day)[rule.rule_start_end == RebalancingDay.LAST_DAY_OF_PERIOD]
    return rebalancing_day


def get_rebalancing_day_next(as_of_date: date, rule: RebalancingRule) -> date:
    if rule.rule_frequency == RebalancingFrequency.MONTHLY:
        first_day, last_day = first_last_weekday_month(as_of_date.year, as_of_date.month, rule.rule_week_day)

    elif rule.rule_frequency == RebalancingFrequency.QUARTERLY:
        first_day, last_day = first_last_weekday_quarter(as_of_date, rule.rule_week_day)

    else:
        first_day, last_day = first_last_weekday_quarter(as_of_date, rule.rule_week_day)

    rebalancing_day = (first_day, last_day)[rule.rule_start_end == RebalancingDay.LAST_DAY_OF_PERIOD]
    return rebalancing_day


def is_rebalancing_day(as_of_date: date, rule: RebalancingRule) -> bool:
    """ 
    """
    if rule.rule_frequency == RebalancingFrequency.MONTHLY:
        first_day, last_day = first_last_weekday_month(as_of_date.year, as_of_date.month, rule.rule_week_day)

    elif rule.rule_frequency == RebalancingFrequency.QUARTERLY:
        first_day, last_day = first_last_weekday_quarter(as_of_date, rule.rule_week_day)

    else:
        first_day, last_day = first_last_weekday_quarter(as_of_date, rule.rule_week_day)

    rebalancing_day = (first_day, last_day)[rule.rule_start_end == RebalancingDay.LAST_DAY_OF_PERIOD]

    return as_of_date.day == rebalancing_day.day


def rebalance(as_of_date: date) -> Dict[str, Decimal]:
    """ Rebalancing.
    """
    weights = []
    return weights


def screen() -> Iterable[str]:
    """ Screening.
    """
    security_codes = []
    return security_codes
