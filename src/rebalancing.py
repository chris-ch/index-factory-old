"""
Rebalancing.
"""
import logging
import calendar
from datetime import date
from enum import Enum
from typing import Iterable, Dict
from decimal import Decimal


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


def first_last_weekday(year: int, month: int, weekday: WeekDay = WeekDay.MONDAY) -> bool:
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

    return first_weekday, last_weekday


def is_rebalancing_day(as_of_date: date) -> bool:
    """ 
    """
    rebalancing_day = first_last_weekday(as_of_date.year, as_of_date.month)[0]
    return as_of_date.day == rebalancing_day


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
