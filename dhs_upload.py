import os
from datetime import date, datetime, timedelta
from io import BytesIO

import paramiko
from az_pmp_utils import drive
from az_pmp_utils.constants import MAX_SERVU_FILE_COUNT, PHX_TZ
from dotenv import load_dotenv


def get_last_sunday() -> date:
    """
    gets the date of the last sunday

    returns:
        a datetime.date for the last sunday
    """
    today = datetime.now(tz=PHX_TZ).date()
    days_since_sunday = today.weekday() + 1
    return today - timedelta(days=days_since_sunday)


def remove_oldest_file(sftp: paramiko.SFTPClient) -> None:
    """
    removes the oldest file from the current folder in the sftp; maintains the `MAX_SERVU_FILE_COUNT` on the server

    args:
        sftp: paramiko SFTPClient
    """
    files = sftp.listdir_attr()
    if len(files) > MAX_SERVU_FILE_COUNT:
        oldest_file = min(files, key=lambda f: f.st_mtime)  # type: ignore[reportArgumentType] | these files will have st_mtime
        print(f'removing oldest file: {oldest_file.filename}...')
        sftp.remove(oldest_file.filename)
        print('file removed')
    else:
        print(f'{MAX_SERVU_FILE_COUNT} files on servu, none removed')


def upload_latest_dhs_file(sftp: paramiko.SFTPClient, folder: str) -> None:
    """
    uploads the latest standard extract to the DHS sftp

    args:
        sftp: paramiko SFTPClient connected to the DHS sftp
        folder: the google drive folder for the standard extracts
    """
    last_sunday = get_last_sunday()
    file_name = last_sunday.strftime('AZ_%Y%m%d.csv')
    files = sftp.listdir()

    if file_name not in files:
        print(f'{file_name} not found, uploading...')
        extract = drive.lazyframe_from_file_name(file_name=file_name, folder_id=folder, drive_ft='csv', separator='|', infer_schema=False)
        csv_buffer = BytesIO()
        extract.collect().write_csv(csv_buffer, separator='|')
        csv_buffer.seek(0)
        print(f'writing {file_name} to sftp...')
        sftp.putfo(csv_buffer, remotepath=file_name)
        print('file uploaded')
    else:
        print(f'{file_name} found, no upload yet')


if __name__ == '__main__':
    load_dotenv()

    folder = os.environ['STANDARD_EXTRACT_FOLDER']

    sftp_host = os.environ['SERVU_HOST']
    sftp_port = os.environ['SERVU_PORT']
    sftp_user = os.environ['SERVU_USERNAME']
    sftp_password = os.environ['SERVU_PASSWORD']

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=sftp_host, port=int(sftp_port), username=sftp_user, password=sftp_password)
    sftp = ssh.open_sftp()

    upload_latest_dhs_file(sftp, folder)
    remove_oldest_file(sftp)

    sftp.close()
    ssh.close()
