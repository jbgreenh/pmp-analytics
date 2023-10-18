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

today = datetime.datetime.now()
last_month = today.replace(day=1) - datetime.timedelta(days=1)
lm_yr = last_month.year
lm_mo = str(last_month.month).zfill(2)

creds = auth()

def pull_files():
    '''
    pull the proper dispensations and request files
    '''
    file_name = f'AZ_Dispensations_{lm_yr}{lm_mo}.csv'
    ob_file_name = f'AZ_Dispensations_{lm_yr}{lm_mo}_opioid_benzo.csv'

    folder_id = secrets['folders']['dispensations_47']

    service = build('drive', 'v3', credentials=creds)

    try:
        results = service.files().list(q=f"name = '{file_name}' and '{folder_id}' in parents",
                                    supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
        files = results.get('files', [])
        file_id = None
        if files:
            file_id = files[0]['id']
            try:
                request = service.files().get_media(fileId=file_id)
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
    disp = pl.read_csv(file, separator='|', infer_schema_length=100000).lazy()

    try:
        results = service.files().list(q=f"name = '{ob_file_name}' and '{folder_id}' in parents",
                                    supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
        files = results.get('files', [])
        file_id = None
        if files:
            file_id = files[0]['id']
            try:
                request = service.files().get_media(fileId=file_id)
            except HttpError as error:
                print(f'error checking google drive: {error}')
        else:
            print('no file found')

        file = io.BytesIO()
        downloader = MediaIoBaseDownload(file, request)
        
        done = False
        print(f'pulling {ob_file_name} from google drive...')
        while done is False:
            status, done = downloader.next_chunk()
    except HttpError as error:
        print(f'google drive error: {error}')
        file = None

    file.seek(0) # after writing, pointer is at the end of the stream
    ob_disp = pl.read_csv(file, separator='|', infer_schema_length=100000).lazy()

    requests_folder_id = secrets['folders']['patient_requests']

    try:
        results_folder = service.files().list(q=f"name = 'AZ_PtReqByProfile_{lm_yr}{lm_mo}' and '{requests_folder_id}' in parents",
                                    supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
        folders = results_folder.get('files', [])
        if folders:
            requests_folder_id = folders[0]['id']
        else:
            print('folder not found')
            requests_folder_id = None
    except HttpError as error:
        print(f'error checking google drive: {error}')

    try:
        results = service.files().list(q=f"name = 'Prescriber.csv' and '{requests_folder_id}' in parents",
                                    supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
        files = results.get('files', [])
        file_id = None
        if files:
            file_id = files[0]['id']
            try:
                request = service.files().get_media(fileId=file_id)
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
    requests = pl.read_csv(file, separator='|', infer_schema_length=100000).lazy()

    return disp, ob_disp, requests


def scorecard_new_row():
    disp, ob_disp, requests = pull_files()

    lookups = (
        requests
        .select('dea_number','totallookups')
    )

    disps = (
        disp
        .filter(pl.col('state') == 'AZ')
        .select('dea_number')
        .select('dea_number')
        .join(lookups, on='dea_number', how='left')
        .group_by('dea_number')
        .sum()
        .collect()
    )

    n = disps.shape[0]
    n_lookups = disps.filter(pl.col('totallookups') > 0).shape[0]
    n_lookups_per = (n_lookups / n) * 100
    n_lookups_str = f'{round(n_lookups_per, 2)}'
    df_lookups = pl.DataFrame({'n_prescribers': [n], 'n_lookups': [n_lookups], '%': [float(n_lookups_str)]})
    
    ob_disps = (
        ob_disp
        .filter(pl.col('state') == 'AZ')
        .select('dea_number')
        .select('dea_number')
        .join(lookups, on='dea_number', how='left')
        .group_by('dea_number')
        .sum()
        .collect()
    )

    ob_n = ob_disps.shape[0]
    ob_n_lookups = ob_disps.filter(pl.col('totallookups') > 0).shape[0]
    ob_n_lookups_per = (ob_n_lookups / ob_n) * 100
    ob_n_lookups_str = f'{round(ob_n_lookups_per, 2)}'
    ob_df_lookups = pl.DataFrame({'ob_n_prescribers': [ob_n], 'ob_n_lookups': [ob_n_lookups], 'ob_%': [float(ob_n_lookups_str)]})

    m_y_str = last_month.strftime('%b-%y')
    date_col = pl.DataFrame({'date': [m_y_str]})
    combined = pl.concat([date_col, df_lookups, ob_df_lookups], how='horizontal')
    return combined


def update_scorecard_sheet(new_row):
    sheet_id = secrets['files']['scorecard']
    range_name = 'scorecard!A:A'
    service = build('sheets', 'v4', credentials=creds)
    result = service.spreadsheets().values().get(spreadsheetId=sheet_id, range=range_name).execute()
    values = result.get('values', [])

    if values:
        last_row = len(values)
    else:
        last_row = 1
    
    data = [list(row) for row in new_row.rows()]
    data_range = f'scorecard!A{last_row + 1}:{chr(65 + len(data[0]))}{last_row + len(data) + 1}'

    request = service.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=data_range,
        valueInputOption='RAW',
        body={'values': data}
    )
    response = request.execute()


def main():
    new_row = scorecard_new_row()
    update_scorecard_sheet(new_row)

if __name__ == '__main__':
    main()