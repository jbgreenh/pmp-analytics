import polars as pl
import io
import toml
import datetime

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from utils.auth import *

def awarxe(day=None):
    '''get yesterday's date and return the most recent awarxe file from the google drive'''
    if day == None:
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        yesterday_year = yesterday.strftime('%Y')
        yesterday = yesterday.strftime('%Y%m%d')
    else:
        yesterday = day
        yesterday_year = day[0:4]
    
    with open('../secrets.toml', 'r') as f:
        secrets = toml.load(f)
    
    folder_id = secrets['folders']['awarxe']
    file_name = f'AZ_UserEx_{yesterday}.csv'
    
    creds = auth()
    service = build('drive', 'v3', credentials=creds)

    try:
        results_folder = service.files().list(q=f"name = '{yesterday_year}' and '{folder_id}' in parents",
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
    return pl.read_csv(file, separator='|', infer_schema_length=100000).lazy()