"""
Rebalancing.
"""
import itertools
import calendar
from datetime import date, timedelta
from enum import Enum
from typing import Iterable, Dict, Tuple
from decimal import Decimal


class WeekDay(Enum):
    """
    Enum version of calendar week days.
    """
    MONDAY = 'monday'
    TUESDAY = 'tuesday'
    WEDNESDAY = 'wednesday'
    THURSDAY = 'thursday'
    FRIDAY = 'friday'
    SATURDAY = 'saturday'
    SUNDAY = 'sunday'

    def __repr__(self):
        return self.name.lower()

    @property
    def position(self):
        return self.positions[self.value]

    @property
    def positions(self):
        return dict([(day.value, count) for count, day in enumerate(WeekDay)])


class RebalancingFrequency(Enum):
    """
    Available rebalancing frequencies.
    """
    MONTHLY = 'monthly'
    QUARTERLY = 'quarterly'
    ANNUALLY = 'annually'


class RebalancingSide(Enum):
    LAST_DAY_OF_PERIOD = 'last day of period'
    FIRST_DAY_OF_PERIOD = 'first day of period'


class RebalancingRule(object):

    def __init__(self, rule_frequency: RebalancingFrequency, rule_week_day: WeekDay, rule_start_end: RebalancingSide):
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


REBALANCING_QUARTERLY_FIRST_TUESDAY = RebalancingRule(RebalancingFrequency.QUARTERLY, WeekDay.TUESDAY, RebalancingSide.FIRST_DAY_OF_PERIOD)
REBALANCING_QUARTERLY_LAST_TUESDAY = RebalancingRule(RebalancingFrequency.QUARTERLY, WeekDay.TUESDAY, RebalancingSide.LAST_DAY_OF_PERIOD)
REBALANCING_MONTHLY_FIRST_TUESDAY = RebalancingRule(RebalancingFrequency.MONTHLY, WeekDay.TUESDAY, RebalancingSide.FIRST_DAY_OF_PERIOD)
REBALANCING_MONTHLY_LAST_TUESDAY = RebalancingRule(RebalancingFrequency.MONTHLY, WeekDay.TUESDAY, RebalancingSide.LAST_DAY_OF_PERIOD)


def first_last_weekday_month(year: int, month: int, weekday: WeekDay = WeekDay.MONDAY) -> Tuple[date, date]:
    """ Checking rebalancing day.
    """
    weeks = calendar.monthcalendar(year, month)
    first_weekday = 0
    for week in weeks:
        if week[weekday.position] != 0:
            first_weekday = week[weekday.position]
            break

    last_weekday = 0
    for week in reversed(weeks):
        if week[weekday.position] != 0:
            last_weekday = week[weekday.position]
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
        if week[weekday.position] != 0:
            first_weekday = week[weekday.position]
            break

    last_weekday = 0
    for week in reversed(quarter_end_weeks):
        if week[weekday.position] != 0:
            last_weekday = week[weekday.position]
            break

    return date(as_of_date.year, month_start, first_weekday), date(as_of_date.year, month_end, last_weekday)


def get_rebalancing_first_last(as_of_date: date, rule: RebalancingRule) -> Tuple[date, date]:
    if rule.rule_frequency == RebalancingFrequency.MONTHLY:
        first_day, last_day = first_last_weekday_month(as_of_date.year, as_of_date.month, rule.rule_week_day)

    elif rule.rule_frequency == RebalancingFrequency.QUARTERLY:
        first_day, last_day = first_last_weekday_quarter(as_of_date, rule.rule_week_day)

    else:
        first_day, last_day = first_last_weekday_quarter(as_of_date, rule.rule_week_day)

    return first_day, last_day


def get_rebalancing_day_previous(as_of_date: date, rule: RebalancingRule) -> date:
    first_day, last_day = get_rebalancing_first_last(as_of_date, rule)
    rebalancing_day = (first_day, last_day)[rule.rule_start_end == RebalancingSide.LAST_DAY_OF_PERIOD]
    if rebalancing_day > as_of_date:
        rebalancing_day = get_rebalancing_day_previous(as_of_date + timedelta(days=-1), rule)
    return rebalancing_day


def get_rebalancing_day_next(as_of_date: date, rule: RebalancingRule) -> date:
    first_day, last_day = get_rebalancing_first_last(as_of_date, rule)
    rebalancing_day = (first_day, last_day)[rule.rule_start_end == RebalancingSide.LAST_DAY_OF_PERIOD]
    if rebalancing_day < as_of_date:
        rebalancing_day = get_rebalancing_day_next(as_of_date + timedelta(days=1), rule)

    return rebalancing_day


def is_rebalancing_day(as_of_date: date, rule: RebalancingRule) -> bool:
    """ 
    """
    return as_of_date == get_rebalancing_day_next(as_of_date, rule)


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
