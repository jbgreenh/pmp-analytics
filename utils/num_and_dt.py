from calendar import SATURDAY
from datetime import date, timedelta

from utils.constants import AZ_HOLIDAYS


def ordinal(n: int) -> str:
    """
    converts a number to its ordinal version (eg 1 to 1st, 4 to 4th)

    args:
        n: the number to convert

    returns:
        the ordinal string
    """
    if 11 <= (n % 100) <= 13:  # noqa: PLR2004 | not magic
        suffix = 'th'
    else:
        suffix = ['th', 'st', 'nd', 'rd', 'th'][min(n % 10, 4)]
    return f'{n}{suffix}'


def add_business_days(start_date: date, days_to_add: int = 30) -> date:
    """
    add the specified number of business days to a given date

    args:
        start_date: the starting date
        days_to_add: how many business days to add to the `start_date`

    returns:
        returns the date `days_to_add` business days after the `start_date`
    """
    days_added = 0

    while days_added < days_to_add:
        start_date += timedelta(days=1)
        if (start_date.weekday() < SATURDAY) and (start_date not in AZ_HOLIDAYS):
            days_added += 1

    return start_date
