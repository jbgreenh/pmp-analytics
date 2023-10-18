import polars as pl
import datetime
import toml

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from utils.auth import *
from utils.awarxe import *

with open('../secrets.toml', 'r') as f:
        secrets = toml.load(f)

creds = auth()

def pull_inspection_list(file_name=None):
    '''
    pull the proper inspection list
    file_name: a string with the exact name of the file; '09/2023 Unregistered Pharmacists Report'
    '''
    if not file_name:
        today = datetime.datetime.now()
        last_month = today.replace(day=1) - datetime.timedelta(days=1)
        lm_yr = last_month.year
        lm_mo = str(last_month.month).zfill(2)

        file_name = f'{lm_mo}/{lm_yr} Unregistered Pharmacists Report'
    else:
        lm_yr = file_name.split(' ')[0].split('/')[1]
        
    folder_id = secrets['folders']['pharmacist_reg']

    service = build('drive', 'v3', credentials=creds)

    try:
        results_folder = service.files().list(q=f"name = '{lm_yr}' and '{folder_id}' in parents",
                                    supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
        folders = results_folder.get('files', [])
        if folders:
            folder_id = folders[0]['id']
        else:
            print('folder not found')
            folder_id = None
    except HttpError as error:
        print(f'error checking google drive: {error}')

    try:
        results = service.files().list(q=f"name = '{file_name}' and '{folder_id}' in parents",
                                    supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
        files = results.get('files', [])
        file_id = None
        if files:
            file_id = files[0]['id']
            try:
                request = service.files().export_media(fileId=file_id, mimeType='text/csv')
            except HttpError as error:
                print(f'error checking google drive: {error}')
        else:
            print('no file found')

        file = io.BytesIO()
        downloader = MediaIoBaseDownload(file, request)
        
        done = False
        print(f'pulling {file_name} from google drive...')
        while done is False:
            status, done = downloader.next_chunk()
    except HttpError as error:
        print(f'google drive error: {error}')
        file = None

    file.seek(0) # after writing, pointer is at the end of the stream
    return pl.read_csv(file, infer_schema_length=100000).lazy()


def registration():
    aw = awarxe()
    aw = (
        aw
        .with_columns(
            pl.col('professional license number').str.to_uppercase().str.strip_chars()
        )
        .select(
            'professional license number'
        )
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

    pharmacies = (
        pl.scan_csv('data/igov_pharmacy.csv')
        .with_columns(
            pl.col('License/Permit #').str.to_uppercase().str.strip_chars()
        )
        .select(
            'License/Permit #', 'Business Name', 'SubType'
        )
    )

    pharmacists = (
        pl.scan_csv('data/igov_pharmacist.csv')
        .with_columns(
                pl.col('License/Permit #').str.to_uppercase().str.strip_chars()
        )
        .select(
            'License/Permit #', 'First Name', 'Middle Name', 'Last Name', 'Status', 'Phone', 'Email'
        )
    )

    inspection_list = pull_inspection_list()
    final_sheet = (
        inspection_list.with_context(aw)
        .with_columns(
            pl.col('License #').is_in(pl.col('professional license number')).map_dict({True:'YES', False:'NO'}).alias('awarxe')
        )
        .filter(pl.col('awarxe').str.contains('NO'))
        .join(
            pharmacies, left_on='Permit #', right_on='License/Permit #', how='left'
        )
        .join(
            pharmacists, left_on='License #', right_on='License/Permit #', how='left'
        )
        .join(
            manage_pharmacies, left_on='Permit #', right_on='Pharmacy License Number', how='left'
        )
        .select(
            'awarxe', 'SubType', 'Business Name', 'Permit #', 'License #', 
            'Last Insp', 'Notes', 'PharmacyDEA', 'First Name', 'Middle Name',
            'Last Name', 'Status', 'Phone', 'Email'
        )
    )

    return final_sheet


def update_unreg_sheet():
    sheet_id = secrets['files']['unreg_pharmacists']
    range_name = 'pharmacists!A:A'
    service = build('sheets', 'v4', credentials=creds)
    result = service.spreadsheets().values().get(spreadsheetId=sheet_id, range=range_name).execute()
    values = result.get('values', [])

    if values:
        last_row = len(values)
    else:
        last_row = 1
    
    reg = registration()
    data = [list(row) for row in reg.collect().rows()]
    data_range = f'pharmacists!B{last_row + 1}:{chr(65 + len(data[0]))}{last_row + len(data) + 1}'

    request = service.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=data_range,
        valueInputOption='RAW',
        body={'values': data}
    )
    response = request.execute()

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


def main():
    update_unreg_sheet()

if __name__ == '__main__':
    main()