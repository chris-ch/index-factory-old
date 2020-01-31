from datetime import date, datetime
from decimal import Decimal
from typing import Dict, Tuple, List


def parse_daily_prices(lines: List[str]) -> Tuple[date, Dict[str, Decimal]]:
    """
    Importing lines from a CSV with daily prices.

    Expected format:
    Symbol,Date,Open,High,Low,Close,Volume
    A,03-Dec-2018,73.33,74.79,73.19,74.67,4265200

    """
    date_header = 'Date'
    close_header = 'Close'
    symbol_header = 'Symbol'
    date_format = '%d-%b-%Y'
    line_separator = ','
    header = lines[0].strip().split(line_separator)
    first_row = dict(zip(header, lines[1].strip().split(line_separator)))
    print(first_row)
    as_of_date = datetime.strptime(first_row[date_header], date_format)
    prices = {}
    for price_row in lines[1:]:
        row = dict(zip(header, price_row.strip().split(line_separator)))
        prices[row[symbol_header]] = Decimal(row[close_header])

    return as_of_date.date(), prices
