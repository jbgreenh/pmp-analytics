import calendar
from datetime import date, datetime
from pathlib import Path

import polars as pl
from az_pmp_utils import tableau
from az_pmp_utils.constants import PHX_TZ


def mm2() -> None:
    """finishes the medical marijuana audit process. run `mm1.py` and follow instructions there first"""
    today = datetime.now(tz=PHX_TZ).date()
    year = today.year
    if today.month < calendar.JULY:
        year -= 1
        start, end = date(year=year, month=7, day=1), date(year=year, month=12, day=31)
    else:
        start, end = date(year=year, month=1, day=1), date(year=year, month=6, day=30)

    workbook_name = 'mm_audit'
    user_ids_luid = tableau.find_view_luid('UserIDs', workbook_name)
    print(f'luid found: {user_ids_luid}')
    searches_luid = tableau.find_view_luid('Searches', workbook_name)
    print(f'luid found: {searches_luid}')

    print('pulling user ids...')
    user_ids_lf = tableau.lazyframe_from_view_id(user_ids_luid, infer_schema=False)
    users_explode = (
        user_ids_lf
        .drop_nulls('Associated DEA Number(s)')
        .with_columns(
            pl.col('User ID').cast(pl.Int32),
            pl.col('Associated DEA Number(s)').str.replace_all(r'\s', '').str.split(',').alias('dea')
        )
        .explode('dea')
        .sort('Active', descending=True)                            # sorting like this and keeping first will favor active accounts
        .unique(subset=['dea'], keep='first', maintain_order=True)  # but still give search credit for inactive accounts if that's all there is
        .select('User ID', 'dea')
    )

    filters = {
        'search_start_date': start,
        'search_end_date': end,
    }
    print('pulling searches data...')
    searches_lf = tableau.lazyframe_from_view_id(searches_luid, filters=filters, infer_schema=False)
    searches_lf = (
        searches_lf
        .select(
            pl.col('TrueID').cast(pl.Int32),
            pl.col('Distinct count of Search ID').str.replace_all(',', '').cast(pl.Int32).alias('totallookups')
        )
    )

    mm_matches = (
        pl.scan_csv('data/mm_matches_combined.csv', infer_schema_length=10000)
        .with_columns(
            pl.lit('').alias('note')
        )
    )
    mm_manual = (
        pl.scan_csv('data/mm_manual.csv', infer_schema_length=10000)
    )

    mm_combined = pl.concat([mm_matches, mm_manual])
    mm_combined = (
        mm_combined
        .join(users_explode, left_on='DEA Number', right_on='dea', how='left')
        .join(searches_lf, left_on='User ID', right_on='TrueID', how='left')
        .with_columns(
            pl.col('totallookups').fill_null(0)
        )
        .with_columns(
            (pl.col('totallookups') / pl.col('Application Count')).alias('Lookups/Count'),
            (pl.col('Application Count') >= 20).alias('>=20'),  # noqa: PLR2004 | number is explained in col name
            ((pl.col('totallookups') / pl.col('Application Count')) < 0.8).alias('<80% Lookups')  # noqa: PLR2004 | number is explained in col name
        )
        .with_columns(
            (pl.col('>=20') & pl.col('<80% Lookups')).alias('test')
        )
        .drop('User ID')
        .sort(['test', 'Application Count'], descending=[True, True])
    )

    file_path = Path('data/mmq.xlsx')
    mm_combined.collect().write_excel(
        file_path,
        worksheet='mm phys audit',
        conditional_formats={
            '>=20': [{
                'type': 'cell',
                'criteria': 'equal to',
                'value': 'TRUE',
                'format': {'bg_color': '#F4CCCC'}
            }, {
                'type': 'cell',
                'criteria': 'equal to',
                'value': 'FALSE',
                'format': {'bg_color': '#D9EAD3'}
            }],
            '<80% Lookups': [{
                'type': 'cell',
                'criteria': 'equal to',
                'value': 'TRUE',
                'format': {'bg_color': '#F4CCCC'}
            }, {
                'type': 'cell',
                'criteria': 'equal to',
                'value': 'FALSE',
                'format': {'bg_color': '#D9EAD3'}
            }],
            'test': [{
                'type': 'cell',
                'criteria': 'equal to',
                'value': 'TRUE',
                'format': {'bg_color': '#F4CCCC'}
            }, {
                'type': 'cell',
                'criteria': 'equal to',
                'value': 'FALSE',
                'format': {'bg_color': '#D9EAD3'}
            }],
        },
        autofit=True,
        freeze_panes='A2'
    )
    print(f'{file_path} saved')


if __name__ == '__main__':
    mm2()
