import os
import pathlib
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import paramiko
from dotenv import load_dotenv
from googleapiclient.discovery import build

from utils import auth, drive


def get_last_sunday() -> date:
    """
    gets the date of the last sunday

    returns:
        a datetime.date for the last sunday
    """
    today = datetime.now(tz=ZoneInfo('America/Phoenix')).date()
    days_since_sunday = today.weekday() + 1
    return today - timedelta(days=days_since_sunday)


def remove_oldest_file(sftp: paramiko.SFTPClient) -> None:
    """
    removes the oldest file from the current folder in the sftp

    args:
        sftp: paramiko SFTPClient
    """
    # TODO: check that a file needs deleting first
    files = sftp.listdir_attr()
    oldest_file = min(files, key=lambda f: f.st_mtime)  # type:ignore reportArgumentType | these files will have st_mtime
    print(f'removing oldest file: {oldest_file.filename}...')
    sftp.remove(oldest_file.filename)
    print('file removed')


def upload_latest_dhs_file(service, sftp: paramiko.SFTPClient, folder: str) -> None:  # noqa: ANN001 | service is dynamically typed
    """
    uploads the latest standard extract to the DHS sftp

    args:
        service: google drive service
        sftp: paramiko SFTPClient connected to the DHS sftp
        folder: the google drive folder for the standard extracts
    """
    # TODO: check that a file needs uploading first
    last_sunday = get_last_sunday()
    file_name = last_sunday.strftime('AZ_%Y%m%d.csv')
    extract = drive.lazyframe_from_file_name(service, file_name=file_name, folder_id=folder, drive_ft='csv', separator='|', infer_schema=False)
    extract.collect().write_csv(file_name, separator='|')
    print(f'writing {file_name} to sftp...')
    sftp.put(localpath=file_name, remotepath=file_name)
    print('file uploaded')
    pathlib.Path(file_name).unlink()


if __name__ == '__main__':
    load_dotenv()

    creds = auth.auth()
    service = build('drive', 'v3', credentials=creds)

    folder = os.environ['STANDARD_EXTRACT_FOLDER']

    sftp_host = os.environ['SERVU_HOST']
    sftp_port = os.environ['SERVU_PORT']
    sftp_user = os.environ['SERVU_USERNAME']
    sftp_password = os.environ['SERVU_PASSWORD']

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=sftp_host, port=int(sftp_port), username=sftp_user, password=sftp_password)
    sftp = ssh.open_sftp()

    upload_latest_dhs_file(service, sftp, folder)
    remove_oldest_file(sftp)

    sftp.close()
    ssh.close()
