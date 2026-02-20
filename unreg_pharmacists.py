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


def pull_inspection_list(service, file_name: str | None = None) -> pl.LazyFrame:    # noqa: ANN001 | service is dynamically typed
    """
    pull the proper inspection list

    args:
        service: an authorized google drive service
        file_name: a string with the exact name of the file; '09/2023 Unregistered Pharmacists Report'

    returns:
        inspection_list: a LazyFrame with the inspection list to be checked for registration
    """
    if not file_name:
        today = datetime.now(tz=PHX_TZ).date()
        last_month = today.replace(day=1) - timedelta(days=1)
        lm_yr = str(last_month.year)
        lm_mo = str(last_month.month).zfill(2)

        file_name = f'{lm_mo}/{lm_yr} Unregistered Pharmacists Report'
    else:
        lm_yr = file_name.split(' ')[0].split('/')[1]

    folder_id = os.environ['PHARMACIST_REG_FOLDER']

    folder_id = drive.folder_id_from_name(service=service, folder_name=lm_yr, parent_folder_id=folder_id)
    return drive.lazyframe_from_file_name(service=service, file_name=file_name, folder_id=folder_id, drive_ft='sheet', infer_schema=False)


def registration(service, inspection_list: pl.LazyFrame) -> pl.LazyFrame:   # noqa: ANN001 | service is dynamically typed
    """
    check the `inspection list` for registration in awarxe

    args:
        service: an authorized google drive service
        inspection_list: a LazyFrame with the inspection list for to check for registration

    returns:
       final_list: the `inspection_list` checked for registration
    """
    awarxe_license_numbers = (
        drive.awarxe(service=service)
        .with_columns(
            pl.col('professional license number').str.to_uppercase().str.strip_chars()
        )
        .select(
            'professional license number'
        )
        .collect()
    )

    mp_path = Path('data/pharmacies.csv')
    files.warn_file_age(mp_path)
    manage_pharmacies = (
        pl.scan_csv(mp_path)
        .with_columns(
            pl.col('Pharmacy License Number').str.to_uppercase().str.strip_chars(),
            pl.col('DEA').str.to_uppercase().str.strip_chars()
        )
        .rename(
            {'DEA': 'PharmacyDEA'}
        )
        .select(
            'Pharmacy License Number', 'PharmacyDEA'
        )
    )

    lr_path = Path('data/List Request.csv')
    files.warn_file_age(lr_path)
    igov = pl.scan_csv(lr_path, infer_schema=False)

    pharmacies = (
        igov
        .filter(
            pl.col('Type') == 'Pharmacy'
        )
        .with_columns(
            pl.col('License/Permit #').str.to_uppercase().str.strip_chars()
        )
        .select(
            'License/Permit #', 'Business Name', 'SubType'
        )
    )

    pharmacists = (
        igov
        .filter(
            pl.col('Type') == 'Pharmacist'
        )
        .with_columns(
                pl.col('License/Permit #').str.to_uppercase().str.strip_chars(),
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
        .with_columns(
            pl.concat_str(
                [
                    pl.col('City,'),
                    pl.col('State'),
                    pl.col('Zip')
                ],
                separator=' '
            ).alias('CSZ')
        )
        .select(
            'License/Permit #', 'First Name', 'Middle Name', 'Last Name', 'Status', 'Phone', 'Email',
            'Address', 'CSZ'
        )
    )

    return (
        inspection_list
        .with_columns(
            pl.col('License #').is_in(awarxe_license_numbers['professional license number'].to_list()).replace_strict({True: 'YES', False: 'NO'}).alias('awarxe')
        )
        .filter(pl.col('awarxe') == 'NO')
        .join(pharmacies, left_on='Permit #', right_on='License/Permit #', how='left')
        .join(pharmacists, left_on='License #', right_on='License/Permit #', how='left')
        .join(manage_pharmacies, left_on='Permit #', right_on='Pharmacy License Number', how='left')
        .select(
            'awarxe', 'License #', 'Last Insp', 'Notes', 'First Name', 'Middle Name', 'Last Name',
            'Status', 'Phone', 'Email', 'Address', 'CSZ', 'Business Name', 'SubType', 'Permit #', 'PharmacyDEA'
        )
        .sort('License #', 'Permit #')
        .unique()
    )


def update_unreg_sheet(creds: google.oauth2.credentials.Credentials | google.auth.external_account_authorized_user.Credentials, registration: pl.LazyFrame) -> None:
    """
    update the unregistered pharmacists sheet with the `registration` list

    args:
        creds: google drive credentials from `auth.auth()`
        registration: a LazyFrame with the registration status of this month's `inspection list`
    """
    sheet_id = os.environ['UNREG_PHARMACISTS_FILE']
    range_name = 'pharmacists!B:B'
    service = build('sheets', 'v4', credentials=creds)
    result = service.spreadsheets().values().get(spreadsheetId=sheet_id, range=range_name).execute()
    values = result.get('values', [])

    last_row = len(values) if values else 1

    data = [list(row) for row in registration.collect().rows()]

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

    inspection_list = pull_inspection_list(service)
    reg = registration(service, inspection_list)
    update_unreg_sheet(creds, reg)
