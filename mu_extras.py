import sys
import calendar
from datetime import date

import polars as pl
import toml
from googleapiclient.discovery import build

from utils import auth, drive, deas


TOP_PRESCRIBERS = 20    # number of prescribers with the most dispensations and no searches


def ordinal(n: int):
    """
    converts a number to its ordinal version (eg 1 to 1st, 4 to 4th)

    args:
        `n`: the number to convert

    returns:
        the ordinal string
    """
    if 11 <= (n % 100) <= 13:
        suffix = 'th'
    else:
        suffix = ['th', 'st', 'nd', 'rd', 'th'][min(n % 10, 4)]
    return str(n) + suffix

def input_str_to_date(month_name:str, year_str:str) -> date:
    """
    converts the input string into a date at the first of the month
    eg january2024 to date(2024, 1, 1)

    args:
        `month_name`: the month name in lowercase: `january`
        `year_str`: the year in YYYY format

    returns:
        a date with the first of the month for the day
    """
    month_num = list(calendar.month_name).index(month_name.title())
    return date(int(year_str), month_num, 1)

def update_appearances(creds, sheet_id:str, update_appearances:pl.LazyFrame):
    """
    updates the appearances google sheet with the `new_appearances`

    args:
        `creds`: google api credentials from auth()
        `sheet_id`: the id of the appearances sheet to update
        `new_appearances`: a lazyframe with the new data
    """
    range_name = 'appearances!A:B'
    service = build('sheets', 'v4', credentials=creds)
    service.spreadsheets().values().clear(spreadsheetId=sheet_id,range=range_name).execute()
    data = [list(row) for row in update_appearances.collect().rows()]
    data.insert(0, ['final_id', 'appearance_date'])
    body = {
        'values': data
    }
    result = service.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=range_name,
        valueInputOption='RAW',
        body=body
    ).execute()
    print(f"{result.get('updatedCells')} cells updated.")

def process_mu(appearance_month:date, input_file:str):
    """
    processes the mu results to remove prescribers notified as not in violation
    and adds information on how many times the prescriber has appeared on the
    mu list

    args:
        `appearance_month`: a `date` with the month of this appearance
        `input_file`: the name of the mu file to add this information to
    """
    month_num = appearance_month.month
    year_str = str(appearance_month.year)

    with open('secrets.toml', 'r') as f:
        secrets = toml.load(f)

    creds = auth.auth()
    service = build('drive', 'v3', credentials=creds)

    no_violation = (
        drive.lazyframe_from_id_and_sheetname(
            service=service, file_id=secrets['files']['not_violation'], sheet_name='no_violation', infer_schema_length=10000
        )
        .with_columns(
            pl.col('exclude until').str.to_date(format='%m/%d/%Y'),
            pl.col('final_id').cast(pl.String)
        )
        .filter(
            pl.col('exclude until') > date.today()
        )
    )

    mu_nv = (
        pl.scan_csv(f'data/{input_file}.csv')
        .join(no_violation, on='final_id', how='anti')
    )

    new_appear = (
        mu_nv
        .head(TOP_PRESCRIBERS)
        .select(['final_id'])
        .with_columns(
            pl.lit(appearance_month).alias('appearance_date')
        )
    )

    appear = (
        drive.lazyframe_from_id_and_sheetname(service=service, file_id=secrets['files']['appearances'], sheet_name='appearances')
        .filter(
            pl.col('appearance_date').str.to_date('%Y-%-m-%-d')
        )
    )

    appear_combine = (
        pl.concat([appear, new_appear])
        # .with_columns(
        #     pl.col('appearance_date').cast(pl.String)
        # )
    )

    # total appearance count, including current report
    appear_counts = (
        appear_combine
        .group_by('final_id')
        .len()
        .rename({'len':'appearance'})
    )

    # last appearance before this report
    last_appear = (
        appear
        .group_by('final_id')
        .max()
        .rename({'appearance_date':'last_appearance'})
    )

    appear_stats = (
        appear_counts.join(last_appear, on='final_id', how='left', coalesce=True)
        .with_columns(
            pl.col('appearance').map_elements(lambda x: ordinal(x), return_dtype=pl.String),
            pl.col('last_appearance').dt.strftime('%B %Y')
        )
    )

    dea = (
        deas.deas('presc')
        .select('DEA Number', 'State License Number')
    )

    filepath = f'data/{input_file}+.csv'
    _output = (
        mu_nv
        .join(appear_stats, on='final_id', how='left', coalesce=True)
        .select(
            pl.lit(str(month_num).zfill(2) + '/' + year_str).alias('MM/YYYY'),
            pl.all()
        )
        .join(dea, left_on='final_id', right_on='DEA Number', how='left', coalesce=True)
        .with_columns(
            pl.col('license_number').fill_null(pl.col('State License Number'))
        )
        .drop('State License Number')
        .collect()
        .head(TOP_PRESCRIBERS)
        .write_csv(filepath)
    )

    print(f'{filepath} written')

    update_appearances(creds, secrets['files']['appearances'], update_appearances=appear_combine)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('please provide an argument')
        print('python mu_extras.py january2024')
        sys.exit(1)
    month_name = ''
    year_str = ''
    for c in sys.argv[1]:
        if c.isalpha():
            month_name += c
        if c.isnumeric():
            year_str += c
    if (month_name.title() not in calendar.month_name) or (len(year_str) != 4):
        print('please follow the below format')
        print('python mu_extras.py january2024')
        sys.exit(1)

    input_file = f'{sys.argv[1]}_mandatory_use_full'

    appearance_month = input_str_to_date(month_name, year_str)
    process_mu(appearance_month, input_file)

