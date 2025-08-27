from typing import Literal

import polars as pl

# ruff: noqa: ERA001
# commented code is for alternate dea file formats

type DeaSelector = Literal['all', 'presc', 'pharm', 'az']


def deas(p: DeaSelector = 'all') -> pl.LazyFrame:  # noqa: RET503 | function returns for all possible p values
    """
    returns a lazyframe from the full dea fixed width file

    p:
    for az prescribers: presc
    for az pharmacies: pharm
    for all az registrants: az
    for all registrants: all

    returns:
    a lazyframe filtered as indicated through `p`
    """
    # widths and names according to the file format specifications provided by the DEA
    # dea_widths = [9, 1, 16, 8, 40, 40, 40, 40, 33, 2, 5, 2, 1, 8, 10, 20, 20]
    dea_widths = [9, 1, 2, 40, 40, 40, 40, 33, 2, 9, 8, 8, 12, 3, 9, 13, 15, 15]

    # dea_names = [
    #     'DEA Number', 'Business Activity Code', 'Drug Schedules', 'Expiration Date', 'Name',
    #     'Additional Company Info', 'Address 1', 'Address 2', 'City', 'State', 'Zip Code', 'Business Activity Sub Code',
    #     'Payment Indicator', 'Activity', 'Degree', 'State License Number', 'State CS License Number'
    #     ]

    dea_names = [
        'DEA Number', 'Business Activity Code', 'Business Activity Sub Code', 'Name', 'Address 1',
        'Address 2', 'Address 3', 'City', 'State', 'Zip Code', 'Date of Original Registration', 'Expiration Date', 'Drug Schedules',
        'Degree', 'SSN', 'Tax ID', 'State License Number', 'State CS License Number'
        ]

    slice_tuples = []
    offset = 0

    for w in dea_widths:
        slice_tuples.append((offset, w))
        offset += w

    # using unit separator '\x1F' to trick pyarrow into only making one col, unlikely to make its way into this latin-1 file
    deas = pl.read_csv('data/cs_active.txt', encoding='latin-1', has_header=False, new_columns=['full_str'], use_pyarrow=True, separator='\x1F')

    deas = (
        deas
        .with_columns(
            [pl.col('full_str').str.slice(slice_tuple[0], slice_tuple[1]).str.strip_chars().alias(col) for slice_tuple, col in zip(slice_tuples, dea_names, strict=False)]
        )
        .drop('full_str')
    )

    if p == 'pharm':
        deas_pharm = (
            deas
            .filter(
                (pl.col('State') == 'AZ') &
                (pl.col('Business Activity Code') == 'A')
            )
        )
        print(deas_pharm.head())
        return deas_pharm.lazy()
    if p == 'presc':
        sub_codes = ['5', '6', '7', '8', 'A', 'B', 'C', 'D', 'J']
        deas_presc = (
            deas
            .filter(
                (pl.col('State') == 'AZ') &
                ((pl.col('Business Activity Code') == 'C') | ((pl.col('Business Activity Code') == 'M') & pl.col('Business Activity Sub Code').is_in(sub_codes)))
            )
        )
        print(deas_presc.head())
        return deas_presc.lazy()
    if p == 'az':
        deas_az = (
            deas
            .filter(
                pl.col('State') == 'AZ'
            )
        )
        print(deas_az.head())
        return deas_az.lazy()
    if p == 'all':
        print(deas.head())
        return deas.lazy()
