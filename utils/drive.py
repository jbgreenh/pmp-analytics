import datetime
import io
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal
from zoneinfo import ZoneInfo

import polars as pl
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

from utils import auth
from utils.constants import EARLIEST_AWARXE_DATE, PHX_TZ

if TYPE_CHECKING:
    from pathlib import Path

type DriveFileType = Literal['sheet', 'csv']


class GoogleDriveHttpError(Exception):
    """custom exception for google drive http errors"""
    def __init__(self, message: str = 'google drive http error') -> None:
        """initializes the error"""
        self.message = message
        super().__init__(self.message)


class GoogleDriveNotFoundError(Exception):
    """custom exception for google drive file not found errors"""
    def __init__(self, message: str = 'google drive not found error') -> None:
        """initializes the error"""
        self.message = message
        super().__init__(self.message)


def lazyframe_from_file_name(file_name: str, folder_id: str, drive_ft: DriveFileType, service=None, **kwargs) -> pl.LazyFrame:  # noqa: ANN001 | service is dynamically typed
    """
        return a lazyframe of the csv in the provided folder

    args:
        file_name: the file name of the csv
        folder_id: the id of the parent folder of the csv
        drive_ft: a `DriveFileType` indicating whethere the file is a sheet or a csv
        service: an authorized google drive service
        **kwargs: kwargs for `pl.read_csv()`

    raises:
        GoogleDriveHttpError : raised when accessing google drive leads to an HttpError
        GoogleDriveNotFoundError : raised when the file is not found on the google drive

    returns:
        a pl.LazyFrame with the contents of the csv
    """
    if service is None:
        service = build('drive', 'v3', credentials=auth.auth())

    try:
        results = service.files().list(q=f"name = '{file_name}' and '{folder_id}' in parents",
                                    supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
        files = results.get('files', [])
        file_id = None
        if files:
            file_id = files[0]['id']
            try:
                if drive_ft == 'csv':
                    request = service.files().get_media(fileId=file_id)
                elif drive_ft == 'sheet':
                    request = service.files().export_media(fileId=file_id, mimeType='text/csv')
            except HttpError as error:
                msg = f'error checking google drive: {error!r}'
                raise GoogleDriveHttpError(msg) from error
        else:
            msg = f'no file found with name: {file_name!r} in folder with id: {folder_id!r}'
            raise GoogleDriveNotFoundError(msg)

        file = io.BytesIO()
        downloader = MediaIoBaseDownload(file, request)

        done = False
        print(f'pulling {file_name} from google drive...')
        while done is False:
            _status, done = downloader.next_chunk()
    except HttpError as error:
        msg = f'google drive error: {error!r}'
        raise GoogleDriveHttpError(msg) from error

    file.seek(0)  # after writing, pointer is at the end of the stream
    return pl.scan_csv(file, **kwargs)


@dataclass
class LatestFile:
    """
    a dataclass containing a lazyframe and the created at timestamp from google drive

    attributes:
        lf: the lazyframe
        created_at: the created at timestamp in phx time
    """
    lf: pl.LazyFrame
    created_at: datetime.datetime


def get_latest_uploaded(folder_id: str, drive_ft: DriveFileType, service=None, **kwargs) -> LatestFile:  # noqa: ANN001 | service is dynamically typed
    """
    get the latest uploaded file in the google drive folder at `folder_id`

    args:
        folder_id: the id of the folder to check
        drive_ft: a DriveFileType indicating whether to check for csvs or sheets
        service: a google drive service
        **kwargs: kwargs to pass to `pl.scan_csv()`

    raises:
        GoogleDriveHttpError : raised when accessing google drive leads to an HttpError
        GoogleDriveNotFoundError : raised when the file is not found on the google drive

    returns:
        a LatestFile object containing a lazyframe and the created time
    """
    if service is None:
        service = build('drive', 'v3', credentials=auth.auth())

    try:
        results = service.files().list(q=f"'{folder_id}' in parents and trashed=false",
            supportsAllDrives=True, includeItemsFromAllDrives=True,
            orderBy="createdTime desc",
            fields="files(id, name, createdTime)"
        ).execute()

        files = results.get('files', [])
        file_id = None
        if files:
            file_id = files[0]['id']
            file_name = files[0]['name']

            file_ct = files[0]['createdTime']
            file_ts = datetime.datetime.strptime(file_ct, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=ZoneInfo('UTC'))
            phx_ts = file_ts.astimezone(PHX_TZ)

            try:
                if drive_ft == 'csv':
                    request = service.files().get_media(fileId=file_id)
                elif drive_ft == 'sheet':
                    request = service.files().export_media(fileId=file_id, mimeType='text/csv')
            except HttpError as error:
                msg = f'error checking google drive: {error}'
                raise GoogleDriveHttpError(msg) from error
        else:
            msg = f'no files found in folder with id: {folder_id!r}'
            raise GoogleDriveNotFoundError(msg)

        file = io.BytesIO()
        downloader = MediaIoBaseDownload(file, request)

        done = False
        print(f'pulling {file_name} from google drive...')
        while done is False:
            _status, done = downloader.next_chunk()
    except HttpError as error:
        msg = f'google drive error: {error!r}'
        raise GoogleDriveHttpError(msg) from error

    file.seek(0)  # after writing, pointer is at the end of the stream
    return LatestFile(lf=pl.scan_csv(file, **kwargs), created_at=phx_ts)


def lazyframe_from_id_and_sheetname(file_id: str, sheet_name: str, service=None, **kwargs) -> pl.LazyFrame:  # noqa: ANN001 | service is dynamically typed
    """
        return a lazyframe given a `file_id` and `sheet_name`

    args:
        file_id: this id of the file
        sheet_name: the sheet name from within the file
        service: an authorized google drive service
        **kwargs: kwargs for `pl.read_excel()`

    raises:
        GoogleDriveHttpError : raised when accessing google drive leads to an HttpError

    returns:
        a pl.LazyFrame with the contents of the sheet
    """
    if service is None:
        service = build('drive', 'v3', credentials=auth.auth())

    try:
        request = service.files().export_media(fileId=file_id, mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except HttpError as error:
        msg = f'error checking google drive: {error!r}'
        raise GoogleDriveHttpError(msg) from error
    file = io.BytesIO()
    downloader = MediaIoBaseDownload(file, request)

    done = False
    print(f'pulling {file_id} sheet {sheet_name} from google drive...')
    while done is False:
        _status, done = downloader.next_chunk()

    file.seek(0)  # after writing, pointer is at the end of the stream
    return pl.read_excel(file, sheet_name=sheet_name, **kwargs).lazy()


def awarxe(day: datetime.date | None = None, service=None) -> pl.LazyFrame:   # noqa: ANN001 | service is dynamically typed
    """
        return a lazy frame of the most recent awarxe file from the google drive, unless day is specified

    args:
        day: the day for the awarxe file
        service: an authorized google drive service

    raises:
        GoogleDriveHttpError : raised when accessing google drive leads to an HttpError
        GoogleDriveNotFoundError : raised when the file is not found on the google drive

    returns:
       awarxe: a lazyframe with all active awarxe registrants from the most recent file as of `day` if specified, or yesterday if `day` is not specified
    """
    if service is None:
        service = build('drive', 'v3', credentials=auth.auth())

    yesterday = datetime.datetime.now(tz=PHX_TZ).date() - datetime.timedelta(days=1)
    day = max(min(day or yesterday, yesterday), EARLIEST_AWARXE_DATE)

    load_dotenv()

    while True:
        file_name = f'AZ_UserEx_{day.strftime('%Y%m%d')}.csv'
        print(f'pulling awarxe file {file_name}...')

        try:
            results_folder = service.files().list(q=f"name = '{day.year}' and '{os.environ.get('AWARXE_FOLDER')}' in parents",
                                        supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
            folders = results_folder.get('files', [])
            if folders:
                year_folder_id = folders[0]['id']
            else:
                msg = f'folder {day.year!r} not found'
                if (day.month == 1) and (day.day == 1):
                    print(msg)
                    day -= datetime.timedelta(days=1)
                    continue
                raise GoogleDriveNotFoundError(msg)
        except HttpError as error:
            msg = f'error checking google drive: {error!r}'
            raise GoogleDriveHttpError(msg) from error

        try:
            results = service.files().list(q=f"name = '{file_name}' and '{year_folder_id}' in parents",
                                        supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
            files = results.get('files', [])
            if files:
                try:
                    request = service.files().get_media(fileId=files[0]['id'])
                except HttpError as error:
                    msg = f'error checking google drive: {error!r}'
                    raise GoogleDriveHttpError(msg) from error
                file = io.BytesIO()
                downloader = MediaIoBaseDownload(file, request)

                done = False
                print(f'pulling {file_name} from google drive...')
                while done is False:
                    _status, done = downloader.next_chunk()
                file.seek(0)  # after writing, pointer is at the end of the stream
                return pl.scan_csv(file, separator='|', infer_schema=False)
            print(f'{file_name} not found')
            day -= datetime.timedelta(days=1)

        except HttpError as error:
            msg = f'error checking google drive: {error!r}'
            raise GoogleDriveHttpError(msg) from error


def folder_id_from_name(folder_name: str, parent_folder_id: str, service=None, *, create: bool = False) -> str:  # noqa: ANN001 | service is dynamically typed
    """
        returns the `folder_id` of the `folder_name` in the parent folder

    args:
        folder_name: the name of the folder
        parent_folder_id: the id of the parent folder
        service: an authorized google drive service
        create: whether to create the folder or not if it doesn't exist

    raises:
        GoogleDriveHttpError : raised when accessing google drive leads to an HttpError
        GoogleDriveNotFoundError : raised when the file is not found on the google drive

    returns:
       folder_id: the id of the folder with `folder_name`
    """
    if service is None:
        service = build('drive', 'v3', credentials=auth.auth())

    try:
        results_folder = service.files().list(q=f"name = '{folder_name}' and '{parent_folder_id}' in parents",
                                    supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
        folders = results_folder.get('files', [])
        if folders:
            folder_id = folders[0]['id']
        else:
            msg = f'folder {folder_name!r} not found'
            if create:
                file_metadata = {
                    'name': folder_name,
                    'parents': [parent_folder_id],
                    'mimeType': 'application/vnd.google-apps.folder',
                }
                folder = service.files().create(supportsAllDrives=True, body=file_metadata).execute()
                folder_url = f"https://drive.google.com/drive/folders/{folder['id']}"
                print(f'folder created at {folder_url}')
                return folder['id']
            raise GoogleDriveNotFoundError(msg)
    except HttpError as error:
        msg = f'error checking google drive: {error!r}'
        raise GoogleDriveHttpError(msg) from error
    else:
        return folder_id


def upload_csv_as_sheet(file_path: Path, folder_id: str, service=None) -> None:  # noqa: ANN001 | service is dynamically typed
    """
        uploads a local csv file as a sheet to the specified folder, `file_name` is the path to the local csv

        removes the extension for the name of the sheet
        eg. 'file.csv' -> 'file'
        you may want to remove the csv after this upload for cleanliness or use a tempfile

    raises:
        GoogleDriveHttpError : raised when accessing google drive leads to an HttpError

    args:
        file_path: the path to the local csv for uploading
        folder_id: the id of the folder to upload to
        service: an authorized google drive service
    """
    if service is None:
        service = build('drive', 'v3', credentials=auth.auth())

    try:
        no_ext = file_path.stem

        file_metadata = {
            'name': no_ext,
            'parents': [folder_id],
            'mimeType': 'application/vnd.google-apps.spreadsheet'
        }

        media = MediaFileUpload(file_path,
                                mimetype='text/csv')

        print(f'uploading {no_ext} to google drive...')

        file = service.files().create(body=file_metadata,
                                      media_body=media,
                                      supportsAllDrives=True,
                                      fields='webViewLink').execute()
        print(f'uploaded to: {file.get("webViewLink")}')

    except HttpError as error:
        msg = f'an error occurred: {error!r}'
        raise GoogleDriveHttpError(msg) from error


def update_sheet(file_path: Path, file_id: str, service=None) -> None:    # noqa: ANN001 | service is dynamically typed
    """
        uses the contents of a local csv file to update the sheet at the specified `file_id`

        you may want to remove the csv after this upload for cleanliness

    args:
        file_path: the path to the local csv file to use for updating
        file_id: the id of the file to be updated
        service: an authorized google drive service

    raises:
        GoogleDriveHttpError : raised when accessing google drive leads to an HttpError
    """
    if service is None:
        service = build('drive', 'v3', credentials=auth.auth())

    try:
        media = MediaFileUpload(file_path,
                                mimetype='text/csv')

        print(f'updating {file_id} with {file_path}...')

        file = service.files().update(fileId=file_id,
                                      media_body=media,
                                      supportsAllDrives=True,
                                      fields='webViewLink').execute()
        print(f'uploaded to: {file.get("webViewLink")}')

    except HttpError as error:
        msg = f'an error occurred: {error!r}'
        raise GoogleDriveHttpError(msg) from error
