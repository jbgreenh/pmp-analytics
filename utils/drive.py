import datetime
import io
import sys
import os

import polars as pl
from dotenv import load_dotenv
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload


def lazyframe_from_file_name_csv(service, file_name:str, folder_id:str, **kwargs) -> pl.LazyFrame:
    """
        return a lazyframe of the csv in the provided folder

    args:
        service: an authorized google drive service
        file_name: the file name of the csv
        folder_id: the id of the parent folder of the csv
        **kwargs: kwargs for `pl.read_csv()`

    returns:
        a pl.LazyFrame with the contents of the csv
        
    """
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
                sys.exit(f'error checking google drive: {error}')
        else:
            sys.exit('no file found')

        file = io.BytesIO()
        downloader = MediaIoBaseDownload(file, request)

        done = False
        print(f'pulling {file_name} from google drive...')
        while done is False:
            _status, done = downloader.next_chunk()
    except HttpError as error:
        sys.exit(f'google drive error: {error}')

    file.seek(0) # after writing, pointer is at the end of the stream
    return pl.read_csv(file, **kwargs).lazy()


def lazyframe_from_file_name_sheet(service, file_name:str, folder_id:str, **kwargs) -> pl.LazyFrame:
    """
        return a lazyframe of the sheet in the provided folder

    args:
        service: an authorized google service
        file_name: the file name of the google sheet
        folder_id: the parent folder id
        **kwargs: kwargs for `pl.read_csv()`

    returns:
       a pl.LazyFrame with the contents of the sheet
    """
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
                sys.exit(f'error checking google drive: {error}')
        else:
            sys.exit('no file found')

        file = io.BytesIO()
        downloader = MediaIoBaseDownload(file, request)

        done = False
        print(f'pulling {file_name} from google drive...')
        while done is False:
            _status, done = downloader.next_chunk()
    except HttpError as error:
        sys.exit(f'google drive error: {error}')

    file.seek(0) # after writing, pointer is at the end of the stream
    return pl.read_csv(file, **kwargs).lazy()


def lazyframe_from_id_and_sheetname(service, file_id:str, sheet_name:str, **kwargs) -> pl.LazyFrame:
    """
        return a lazyframe given a `file_id` and `sheet_name`

    args:
        service: an authorized google drive service
        file_id: this id of the file
        sheet_name: the sheet name from within the file
        **kwargs: kwargs for `pl.read_excel()`

    returns:
        a pl.LazyFrame with the contents of the sheet
    """
    try:
        request = service.files().export_media(fileId=file_id, mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except HttpError as error:
        sys.exit(f'error checking google drive: {error}')
    file = io.BytesIO()
    downloader = MediaIoBaseDownload(file, request)

    done = False
    print(f'pulling {file_id} sheet {sheet_name} from google drive...')
    while done is False:
        _status, done = downloader.next_chunk()

    file.seek(0) # after writing, pointer is at the end of the stream
    return pl.read_excel(file, sheet_name=sheet_name, **kwargs).lazy()


def awarxe(service, day:str='') -> pl.LazyFrame:
    """
        return a lazy frame of the most recent awarxe file from the google drive, unless day is specified
        
    args:
        service: an authorized google drive service
        day: the day for the awarxe file in %Y%m%d format

    returns:
       awarxe: a lazyframe with all active awarxe registrants as of `day` if specified or yesterday if `day` is not specified
    """
    if day == '':
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        yesterday_year = yesterday.strftime('%Y')
        yesterday = yesterday.strftime('%Y%m%d')
    else:
        yesterday = day
        yesterday_year = day[0:4]

    load_dotenv()

    folder_id = os.environ.get('AWARXE_FOLDER')
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
        sys.exit(f'error checking google drive: {error}')

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
                sys.exit(f'error checking google drive: {error}')
        else:
            sys.exit('no file found')


        file = io.BytesIO()
        downloader = MediaIoBaseDownload(file, request)

        done = False
        print(f'pulling {file_name} from google drive...')
        while done is False:
            _status, done = downloader.next_chunk()
    except HttpError as error:
        sys.exit(f'google drive error: {error}')

    file.seek(0) # after writing, pointer is at the end of the stream
    return pl.scan_csv(file, separator='|', infer_schema_length=100000)


def folder_id_from_name(service, folder_name:str, parent_id:str) -> str:
    """
        returns the `folder_id` of the `folder_name` in the parent folder

    args:
        service: an authorized google drive service
        folder_name: the name of the folder
        parent_id: the id of the parent folder

    returns:
       folder_id: the id of the folder with `folder_name`
    """
    try:
        results_folder = service.files().list(q=f"name = '{folder_name}' and '{parent_id}' in parents",
                                    supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
        folders = results_folder.get('files', [])
        if folders:
            folder_id = folders[0]['id']
        else:
            sys.exit('folder not found')

        return folder_id
    except HttpError as error:
        sys.exit(f'error checking google drive: {error}')


def upload_csv_as_sheet(service, file_name:str, folder_id:str) -> None:
    """
        uploads a local csv file as a sheet to the specified folder, `file_name` is the path to the local csv
        removes the extension for the name of the sheet
        eg. 'file.csv' -> 'file'
        you may want to remove the csv after this upload for cleanliness

    args:
        service: an authorized google drive service
        file_name: the path to the local csv for uploading
        folder_id: the id of the folder to upload to
    """
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
        sys.exit(f'an error occurred: {error}')


def update_sheet(service, file_name:str, file_id:str) -> None:
    """
        uses the contents of a local csv file to update the sheet at the specified `file_id`
        you may want to remove the csv after this upload for cleanliness

    args:
        service: an authorized google drive service
        file_name: the path to the local csv file to use for updating
        file_id: the id of the file to be updated
    """
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
        sys.exit(f'an error occurred: {error}')


def find_or_create_folder(service, folder_name:str, parent_folder_id:str) -> str:
    """
        goes into the google drive to find a folder with the pharmacy name, if it is not there it will create a new folder
        function will also convert the top_pharmacy csv into a google sheet and transfer it into the correct folder and provide a url link for the folder

    args:
        service: an authorized google drive service
        folder_name: the name of the new folder
        parent_folder_id: the id of the folder where the new folder should go

    returns:
        folder_id: a string that will contain the id of the folder created
    """
    folder_id = None
    try:
        print(f'searching for folder: {folder_name}...')
        results = service.files().list(q=f"name = '{folder_name}' and '{parent_folder_id}' in parents", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
        files = results.get('files', [])
        if files:
            folder_id = files[0]['id']
            folder_url = f"https://drive.google.com/drive/folders/{folder_id}"
            print(f'folder exists at {folder_url}')
            return folder_id
        else:
            file_metadata = {
                'name': folder_name,
                'parents': [parent_folder_id],
                'mimeType': 'application/vnd.google-apps.folder',
            }
            folder = service.files().create(supportsAllDrives=True, body=file_metadata).execute()
            folder_url = f"https://drive.google.com/drive/folders/{folder['id']}"
            print(f'folder created at {folder_url}')
            return folder['id']

    except HttpError as error:
        sys.exit(f'error checking google drive: {error}')
