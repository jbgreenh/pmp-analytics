import re

import polars as pl

from utils import deas


def test_deas() -> None:
    """tests that deas returns a lazyframe with expected data types in key fields"""
    deas_lf = deas.deas('all', az=False)
    assert isinstance(deas_lf, pl.LazyFrame)

    deas_df = deas_lf.collect()

    pattern = r'^[ABCFGHMPRabcfghmpr][A-Za-z](?:[0-9]{6}[1-9]|[0-9]{5}[1-9][0-9]|[0-9]{4}[1-9][0-9]{2}|[0-9]{3}[1-9][0-9]{3}|[0-9]{2}[1-9][0-9]{4}|[0-9][1-9][0-9]{5}|[1-9][0-9]{6})$'
    alt_pat = r'^[AFPRBMafprbm](?:[0-9]{8})'
    for dea in set(deas_df['DEA Number']):
        assert re.match(pattern, dea) or re.match(alt_pat, dea)

    for ac in set(deas_df['Business Activity Code']):
        assert ac.isalpha()

    for state in set(deas_df['State']):
        assert state.isalpha()

    for zip_code in set(deas_df['Zip Code']):
        assert zip_code.isnumeric()

    valid_schedule_characters = {'5', 'N', ' ', '3', '4', '2', 'L', '1'}
    for schedules in set(deas_df['Drug Schedules']):
        for c in schedules:
            assert c in valid_schedule_characters

    for ssn in set(deas_df['SSN']):
        assert ssn.isnumeric() or not ssn
