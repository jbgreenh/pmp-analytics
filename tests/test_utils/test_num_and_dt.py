from datetime import date

from utils import num_and_dt


def test_ordinal() -> None:
    """test ordinal function"""
    assert num_and_dt.ordinal(1) == '1st'
    assert num_and_dt.ordinal(2) == '2nd'
    assert num_and_dt.ordinal(3) == '3rd'
    assert num_and_dt.ordinal(4) == '4th'
    assert num_and_dt.ordinal(12) == '12th'
    assert num_and_dt.ordinal(102) == '102nd'
    assert num_and_dt.ordinal(109) == '109th'


def test_add_business_days() -> None:
    """test add_business_days function"""
    assert num_and_dt.add_business_days(date(2025, 12, 24), 1) == date(2025, 12, 26)  # holiday
    assert num_and_dt.add_business_days(date(2025, 12, 26), 1) == date(2025, 12, 29)  # weekends
    assert num_and_dt.add_business_days(date(2025, 12, 29), 1) == date(2025, 12, 30)  # normal weekdays
