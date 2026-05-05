import os
from datetime import datetime, timedelta

# from pathlib import Path
import polars as pl
from az_pmp_utils import drive
from dotenv import load_dotenv

from constants import PHX_TZ

load_dotenv()

awarxe_license_numbers = (
    drive.awarxe()
    .select(
        pl.col('professional license number').str.strip_chars().str.to_uppercase()
    )
    .collect()
    ['professional license number']
    .to_list()
)

mp_deas = (
    pl.scan_csv('data/pharmacies.csv', infer_schema=False)
    .select(
        pl.col('DEA').alias('dea_number')
    )
    .collect()
    ['dea_number']
    .to_list()
)


list_request = (
    pl.scan_csv('data/List Request.csv', infer_schema=False)
    .with_columns(
        pl.concat_str(
            [
                pl.col('Street Address'),
                pl.col('Apt/Suite #')
            ],
            separator=' '
        ).alias('Address'),
        pl.concat_str(
            [
                pl.col('City'),
                pl.lit(',')
            ]
        ).alias('City,')
    )
    .select(
        pl.col('License/Permit #').str.strip_chars().str.to_uppercase().alias('license_number'),
        pl.col('Status').alias('igov_status'),
        pl.col('Email').alias('email'),
        pl.col('First Name').alias('first_name'),
        pl.col('Middle Name').alias('middle_name'),
        pl.col('Last Name').alias('last_name'),
        pl.col('Phone').alias('phone'),
        pl.col('Street Address').alias('address'),
        pl.concat_str(
            [
                pl.col('City,'),
                pl.col('State'),
                pl.col('Zip')
            ],
            separator=' '
        ).alias('csz'),
        pl.col('Business Name').alias('business_name'),
        pl.col('SubType').alias('subtype'),
    )
)

today = datetime.now(tz=PHX_TZ)
last_mo = (today.replace(day=1) - timedelta(days=1))

license_tracker_file_id = os.environ['PI_LICENSE_TRACKER_FILE']
licenses = (
    drive.lazyframe_from_id_and_sheetname(file_id=license_tracker_file_id, sheet_name='Form Responses 1', infer_schema_length=0)  # read_excel() does not have infer_schema
    .select(
        pl.col('Timestamp').str.to_date('%Y-%m-%d %H:%M:%S%.f').alias('submit_date'),
        pl.col('Permit Number').alias('permit_number'),
        pl.col('License Numbers').str.to_uppercase().str.strip_chars().str.replace_all(r'\s+', '|').str.split('|').alias('license_numbers'),
        pl.col('DEA Number').alias('dea_number')
    )
    .filter(pl.col('submit_date').dt.month() == last_mo.month)
    .explode('license_numbers')
    .rename({'license_numbers': 'license_number'})
    .with_columns(
        pl.col('license_number').is_in(awarxe_license_numbers).replace_strict({True: 'YES', False: 'NO'}).alias('awarxe'),
        pl.col('dea_number').is_in(mp_deas).replace_strict({True: 'YES', False: 'NO'}).alias('dea_in_mp?'),
    )
    .sort('submit_date', 'permit_number')
    .filter(pl.col('awarxe') == 'NO')
    .join(list_request, on='license_number', how='left')
    .select(
        'license_number',
        'submit_date',
        pl.lit('').alias('notes'),
        'first_name',
        'middle_name',
        'last_name',
        'igov_status',
        'phone',
        'email',
        'address',
        'csz',
        'business_name',
        'subtype',
        'permit_number',
        'dea_number',
        'dea_in_mp?',
    )
)

print(licenses.collect())
# TODO: append to end of unregistered pharmacist report
