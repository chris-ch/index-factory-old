from datetime import date, datetime
from decimal import Decimal
from typing import Dict, Tuple, List, Iterable


class LoaderDecimalCSV(object):
    """
    Generic CSV loader.
    """
    def __init__(self,  columns: Iterable[str],
        date_format: str,
        line_separator: str=',',
        date_header: str='Date', 
        symbol_header: str='Symbol'
        ):
        self._columns = columns
        self._date_format = date_format
        self._date_header = date_header
        self._symbol_header = symbol_header
        self._line_separator = line_separator

    def load(self, lines: List[str]) -> Tuple[date, Dict[str, Dict[str, Decimal]]]:
        """
        Loading lines.
        """
        header = lines[0].strip().split(self._line_separator)
        first_row = dict(zip(header, lines[1].strip().split(self._line_separator)))
        as_of_date = datetime.strptime(first_row[self._date_header], self._date_format)
        results = {}
        for column in self._columns:
            results[column] = {}
            
        for price_row in lines[1:]:
            if len(price_row.strip()) == 0:
                continue

            row = dict(zip(header, price_row.strip().split(self._line_separator)))
            for column in self._columns:
                results[column][row[self._symbol_header]] = Decimal(row[column])

        return as_of_date.date(), results


def parse_daily_prices(lines: List[str]) -> Tuple[date, Dict[str, Decimal]]:
    """
    Importing lines from a CSV with daily prices.

    Expected format:
    Symbol,Date,Open,High,Low,Close,Volume
    A,03-Dec-2018,73.33,74.79,73.19,74.67,4265200

    """
    loader = LoaderDecimalCSV(['Close'], '%d-%b-%Y')
    as_of_date, results = loader.load(lines)
    return as_of_date, results['Close']
