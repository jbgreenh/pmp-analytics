import polars as pl
import io
import datetime
import toml
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

def lazyframe_from_file_name_csv(service, file_name, folder_id, sep=','):
    '''
    return a lazyframe of the csv in the provided folder
    '''
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
    return pl.read_csv(file, separator=sep, infer_schema_length=100000).lazy()


def lazyframe_from_filename_sheet(service, file_name, folder_id):
    '''
    return a lazyframe of the sheet in the provided folder
    '''
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


def awarxe(service, day=None):
    '''get yesterday's date and return the most recent awarxe file from the google drive as a lazyframe'''
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


def folder_id_from_name(service, folder_name, parent_id):
    '''
    returns the folder id of the folder_name in the parent folder
    '''
    try:
        results_folder = service.files().list(q=f"name = '{folder_name}' and '{parent_id}' in parents",
                                    supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
        folders = results_folder.get('files', [])
        if folders:
            folder_id = folders[0]['id']
        else:
            print('folder not found')
            folder_id = None

        return folder_id
    except HttpError as error:
        print(f'error checking google drive: {error}')


def upload_csv_as_sheet(service, file_name, folder_id):
    '''
    uploads a local csv file as a sheet to the specified folder, removes the extension for the name of the sheet
    eg. 'file.csv' -> 'file'
    you may want to remove the csv after this upload for clealiness
    '''
    try:
        no_ext = file_name.split('.')[0]
        
        file_metadata = {
            'name': no_ext, 
            'parents':[folder_id],
            'mimeType': 'application/vnd.google-apps.spreadsheet'
        }

        media = MediaFileUpload(file_name,
                                mimetype='text/csv')
        
        print(f'uploading {file_name} to google drive...')

        file = service.files().create(body=file_metadata,
                                      media_body=media,
                                      supportsAllDrives=True,
                                      fields='webViewLink').execute()
        print (f'uploaded to: {file.get("webViewLink")}')
        
    except HttpError as error:
        print(f'an error occurred: {error}')