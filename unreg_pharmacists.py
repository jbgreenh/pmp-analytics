import os
import polars as pl
import datetime

from dotenv import load_dotenv
from googleapiclient.discovery import build
from utils import auth
from utils import drive

def pull_inspection_list(file_name:str|None=None) -> pl.LazyFrame:
    """
    pull the proper inspection list

    args:
        file_name: a string with the exact name of the file; '09/2023 Unregistered Pharmacists Report'
        
    returns:
        inspection_list: a LazyFrame with the inspection list to be checked for registration
    """
    if not file_name:
        today = datetime.datetime.now()
        last_month = today.replace(day=1) - datetime.timedelta(days=1)
        lm_yr = str(last_month.year)
        lm_mo = str(last_month.month).zfill(2)

        file_name = f'{lm_mo}/{lm_yr} Unregistered Pharmacists Report'
    else:
        lm_yr = file_name.split(' ')[0].split('/')[1]

    folder_id = os.environ.get('PHARMACIST_REG_FOLDER')

    folder_id = drive.folder_id_from_name(service=service, folder_name=lm_yr, parent_id=folder_id)
    return drive.lazyframe_from_file_name_sheet(service=service, file_name=file_name, folder_id=folder_id, infer_schema_length=10000)


def registration(inspection_list:pl.LazyFrame) -> pl.LazyFrame:
    """
    check the `inspection list` for registration in awarxe

    args:
        inspection_list: a LazyFrame with the inspection list for to check for registration

    returns:
       final_list: the `inspection_list` checked for registration 
    """
    aw = (
        drive.awarxe(service=service)
        .with_columns(
            pl.col('professional license number').str.to_uppercase().str.strip_chars()
        )
        .select(
            'professional license number'
        )
        .collect()
    )

    manage_pharmacies = (
        pl.scan_csv('data/pharmacies.csv')
        .with_columns(
            pl.col('Pharmacy License Number').str.to_uppercase().str.strip_chars(),
            pl.col('DEA').str.to_uppercase().str.strip_chars()
        )
        .rename(
            {'DEA':'PharmacyDEA'}
        )
        .select(
            'Pharmacy License Number', 'PharmacyDEA'
        )
    )

    igov = pl.scan_csv('data/List Request.csv', infer_schema_length=0)

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

    final_sheet = (
        inspection_list
        .with_columns(
            pl.col('License #').is_in(aw['professional license number'].to_list()).replace_strict({True:'YES', False:'NO'}).alias('awarxe')
        )
        .filter(pl.col('awarxe') == 'NO')
        .join(
            pharmacies, left_on='Permit #', right_on='License/Permit #', how='left', coalesce=True
        )
        .join(
            pharmacists, left_on='License #', right_on='License/Permit #', how='left', coalesce=True
        )
        .join(
            manage_pharmacies, left_on='Permit #', right_on='Pharmacy License Number', how='left', coalesce=True
        )
        .select(
            'awarxe', 'License #', 'Last Insp', 'Notes', 'First Name', 'Middle Name', 'Last Name',
            'Status', 'Phone', 'Email', 'Address', 'CSZ', 'Business Name', 'SubType', 'Permit #', 'PharmacyDEA'
        )
    )

    return final_sheet


def update_unreg_sheet(registration:pl.LazyFrame):
    """
    update the unregistered pharmacists sheet with the `registration` list

    args:
        registration: a LazyFrame with the registration status of this month's `inspection list`
    """
    sheet_id = os.environ.get('UNREG_PHARMACISTS_FILE')
    range_name = 'pharmacists!B:B'
    service = build('sheets', 'v4', credentials=creds)
    result = service.spreadsheets().values().get(spreadsheetId=sheet_id, range=range_name).execute()
    values = result.get('values', [])

    if values:
        last_row = len(values)
    else:
        last_row = 1

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
    inspection_list = pull_inspection_list()
    reg = registration(inspection_list)
    update_unreg_sheet(reg)
