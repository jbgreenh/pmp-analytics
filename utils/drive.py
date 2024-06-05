import polars as pl
import io
import datetime
import toml
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

def lazyframe_from_file_name_csv(service, file_name:str, folder_id:str, **kwargs) -> pl.LazyFrame | None:
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
                return
        else:
            print('no file found')
            return

        file = io.BytesIO()
        downloader = MediaIoBaseDownload(file, request)

        done = False
        print(f'pulling {file_name} from google drive...')
        while done is False:
            _status, done = downloader.next_chunk()
    except HttpError as error:
        print(f'google drive error: {error}')
        return

    file.seek(0) # after writing, pointer is at the end of the stream
    return pl.read_csv(file, **kwargs).lazy()


def lazyframe_from_file_name_sheet(service, file_name:str, folder_id:str, **kwargs) -> pl.LazyFrame | None:
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
                return
        else:
            print('no file found')
            return

        file = io.BytesIO()
        downloader = MediaIoBaseDownload(file, request)

        done = False
        print(f'pulling {file_name} from google drive...')
        while done is False:
            _status, done = downloader.next_chunk()
    except HttpError as error:
        print(f'google drive error: {error}')
        return

    file.seek(0) # after writing, pointer is at the end of the stream
    return pl.read_csv(file, **kwargs).lazy()


def lazyframe_from_id_and_sheetname(service, file_id:str, sheet_name:str, **kwargs) -> pl.LazyFrame | None:
    try:
        request = service.files().export_media(fileId=file_id, mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except HttpError as error:
        print(f'error checking google drive: {error}')
        return
    file = io.BytesIO()
    downloader = MediaIoBaseDownload(file, request)

    done = False
    print(f'pulling {file_id} sheet {sheet_name} from google drive...')
    while done is False:
        _status, done = downloader.next_chunk()

    file.seek(0) # after writing, pointer is at the end of the stream
    return pl.read_excel(file, sheet_name=sheet_name, **kwargs).lazy()


def awarxe(service, day:str='') -> pl.LazyFrame | None:
    '''
    return a lazy frame of the most recent awarxe file from the google drive, unless day is specified
    day should be a string in %Y%m%d format
    '''
    if day == '':
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
                return
        else:
            print('no file found')
            return


        file = io.BytesIO()
        downloader = MediaIoBaseDownload(file, request)

        done = False
        print(f'pulling {file_name} from google drive...')
        while done is False:
            _status, done = downloader.next_chunk()
    except HttpError as error:
        print(f'google drive error: {error}')
        return

    file.seek(0) # after writing, pointer is at the end of the stream
    return pl.read_csv(file, separator='|', infer_schema_length=100000).lazy()


def folder_id_from_name(service, folder_name:str, parent_id:str) -> str | None:
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


def upload_csv_as_sheet(service, file_name:str, folder_id:str) -> None:
    '''
    uploads a local csv file as a sheet to the specified folder, file_name is the path to the local csv
    removes the extension for the name of the sheet
    eg. 'file.csv' -> 'file'
    you may want to remove the csv after this upload for cleanliness
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

        print(f'uploading {no_ext} to google drive...')

        file = service.files().create(body=file_metadata,
                                      media_body=media,
                                      supportsAllDrives=True,
                                      fields='webViewLink').execute()
        print (f'uploaded to: {file.get("webViewLink")}')

    except HttpError as error:
        print(f'an error occurred: {error}')


def update_sheet(service, file_name:str, file_id:str) -> None:
    '''
    uses the contents of a local csv file to update the sheet at the specified file_id
    file_name is the path to the local csv
    you may want to remove the csv after this upload for cleanliness
    '''
    try:
        media = MediaFileUpload(file_name,
                                mimetype='text/csv')

        print(f'updating {file_id} with {file_name}...')

        file = service.files().update(fileId=file_id,
                                      media_body=media,
                                      supportsAllDrives=True,
                                      fields='webViewLink').execute()
        print (f'uploaded to: {file.get("webViewLink")}')

    except HttpError as error:
        print(f'an error occurred: {error}')
