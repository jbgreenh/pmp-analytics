import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
from az_pmp_utils import auth, drive, files
from dotenv import load_dotenv
from googleapiclient.discovery import build

from constants import PHX_TZ

if TYPE_CHECKING:
    import google.auth.external_account_authorized_user
    import google.oauth2.credentials


def check_registration(service) -> pl.LazyFrame:    # noqa: ANN001 | service is dynamically typed
    """
    checks pharmacist license numbers from inspections submissions during the last month for awarxe registrations

    args:
        service: an authorized google drive service

    returns:
        a lazyframe with information ready to update the unreg pharmacist tracking sheet
    """
    awarxe_license_numbers = (
        drive.awarxe(service=service)
        .select(
            pl.col('professional license number').str.strip_chars().str.to_uppercase()
        )
        .collect()
        ['professional license number']
        .to_list()
    )

    mp_path = Path('data/pharmacies.csv')
    files.warn_file_age(mp_path)
    mp_deas = (
        pl.scan_csv(mp_path, infer_schema=False)
        .select(
            pl.col('DEA').alias('dea_number')
        )
        .collect()
        ['dea_number']
        .to_list()
    )

    lr_path = Path('data/List Request.csv')
    files.warn_file_age(lr_path)
    list_request = (
        pl.scan_csv(lr_path, infer_schema=False)
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
    )

    list_request_bus = (
        list_request
        .select(
            pl.col('License/Permit #').str.strip_chars().str.to_uppercase().alias('permit_number'),
            pl.col('Status').alias('igov_status'),
            pl.col('Business Name').alias('business_name'),
            pl.col('SubType').alias('subtype'),
        )

    )

    list_request_per = (
        list_request
        .select(
            pl.col('License/Permit #').str.strip_chars().str.to_uppercase().alias('license_number'),
            pl.col('Email').alias('email'),
            pl.col('Street Address').alias('address'),
            pl.concat_str(
                [
                    pl.col('City,'),
                    pl.col('State'),
                    pl.col('Zip')
                ],
                separator=' '
            ).alias('csz'),
            pl.col('First Name').alias('first_name'),
            pl.col('Middle Name').alias('middle_name'),
            pl.col('Last Name').alias('last_name'),
            pl.col('Phone').alias('phone'),
        )
    )

    today = datetime.now(tz=PHX_TZ)
    last_mo = (today.replace(day=1) - timedelta(days=1))

    license_tracker_file_id = os.environ['PI_LICENSE_TRACKER_FILE']

    inspect_pharmacists = (
        drive.lazyframe_from_id_and_sheetname(file_id=license_tracker_file_id, sheet_name='Form Responses 1', service=service, infer_schema_length=0)  # read_excel() does not have infer_schema
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
    )

    unreg_pharmacists = (
        inspect_pharmacists
        .filter(pl.col('awarxe') == 'NO')
        .join(list_request_bus, on='permit_number', how='left')
        .join(list_request_per, on='license_number', how='left')
        .select(
            'awarxe',
            'license_number',
            pl.col('submit_date').dt.to_string('%m/%d/%Y'),
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
        .sort('submit_date', 'permit_number')
    )

    print(unreg_pharmacists.collect()['awarxe'].value_counts().sort('awarxe'))

    return unreg_pharmacists


def update_unreg_sheet(creds: google.oauth2.credentials.Credentials | google.auth.external_account_authorized_user.Credentials, unregistered_pharmacists: pl.DataFrame) -> None:
    """
    update the unregistered pharmacists sheet with new unregistered pharmacists

    args:
        creds: google drive credentials from `auth.auth()`
        unregistered_pharmacists: a LazyFrame with the unregistered pharmacists submitted with pharmacy inspections in the last month
    """
    sheet_id = os.environ['UNREG_PHARMACISTS_FILE']
    range_name = 'pharmacists!B:B'
    service = build('sheets', 'v4', credentials=creds)
    result = service.spreadsheets().values().get(spreadsheetId=sheet_id, range=range_name).execute()
    values = result.get('values', [])

    last_row = len(values) if values else 1

    data = [list(row) for row in unregistered_pharmacists.rows()]

    data_range = f'pharmacists!B{last_row + 1}:{chr(65 + len(data[0]))}{last_row + len(data) + 1}'

    request = service.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=data_range,
        valueInputOption='RAW',
        body={'values': data}
    )
    _response = request.execute()

    # add checkboxes
    checkbox_request = {
        'requests': [{
                'repeatCell': {
                    'cell': {
                        'dataValidation': {
                            'condition': {
                                'type': 'BOOLEAN'
                            }
                        }
                    },
                    'range': {
                        'sheetId': 0,
                        'startRowIndex': last_row,
                        'endRowIndex': last_row + len(data),
                        'startColumnIndex': 0,
                        'endColumnIndex': 1
                    },
                    'fields': 'dataValidation'
                }
        }]
    }
    service.spreadsheets().batchUpdate(spreadsheetId=sheet_id, body=checkbox_request).execute()

    sheet_link = f'https://docs.google.com/spreadsheets/d/{sheet_id}'
    print(f'appended {len(data)} rows to {sheet_link}')


if __name__ == '__main__':
    load_dotenv()

    creds = auth.auth()
    service = build('drive', 'v3', credentials=creds)

    unreg_pharmacists = check_registration(service)
    update_unreg_sheet(creds, unreg_pharmacists.collect())
