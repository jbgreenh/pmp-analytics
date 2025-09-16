import argparse
import os
import stat
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import paramiko
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload

from utils import auth

UTC_NOW = datetime.now(tz=ZoneInfo('UTC'))


def upload_file(service, sftp: paramiko.SFTPClient, remote_file_path: str, drive_folder_id: str) -> None:  # noqa: ANN001 | service is dynamically typed
    """
    uploads a file to the google drive if it is new or has been modified
    only checks files with an mtime younger than 24 hours

    args:
       service: an authorized google service
       sftp: a connected paramiko SFTPClient
       remote_file_path: the remote file path to the file for potential uploading
       drive_folder_id: the id of the target folder on the google drive
    """
    remote_file = os.path.basename(remote_file_path)  # noqa: PTH119 | paramiko is not compatible with Path

    st_mtime = sftp.lstat(remote_file_path).st_mtime
    remote_file_mtime = datetime.fromtimestamp(float(st_mtime)).astimezone(tz=ZoneInfo('UTC')) if isinstance(st_mtime, int) else datetime(year=2001, month=1, day=1, tzinfo=ZoneInfo('UTC'))
    time_dif = UTC_NOW - remote_file_mtime
    print(f'{remote_file} is {round(time_dif.total_seconds() / 60 / 60, 2)} hours old')

    if time_dif <= timedelta(hours=24):
        try:
            results = service.files().list(q=f"name = '{remote_file}' and '{drive_folder_id}' in parents",
                                        supportsAllDrives=True,
                                        includeItemsFromAllDrives=True,
                                        fields='files(id, modifiedTime)').execute()
            files = results.get('files', [])
            if files:
                print(f'{remote_file} already exists on google drive')
                drive_file_id = files[0]['id']
                drive_file_modified_time = datetime.fromisoformat(files[0]['modifiedTime'])
                if remote_file_mtime > drive_file_modified_time:
                    print(f'{remote_file} has been updated since uploading')
                    print(f'updating {remote_file} on google drive...')
                    with sftp.file(remote_file_path, 'rb') as remote_file_content:
                        remote_file_content.prefetch()
                        media = MediaIoBaseUpload(remote_file_content, mimetype='application/octet-stream', chunksize=1024 * 1024, resumable=True)
                        service.files().update(fileId=drive_file_id, media_body=media, supportsAllDrives=True).execute()
                        print(f'{remote_file} updated on google drive.')
                else:
                    print(f'{remote_file} has not been updated, skipping...')
            else:
                print(f'uploading {remote_file} to google drive...')
                with sftp.file(remote_file_path, 'rb') as remote_file_content:
                    remote_file_content.prefetch()
                    media = MediaIoBaseUpload(remote_file_content, mimetype='application/octet-stream', chunksize=1024 * 1024, resumable=True)
                    file_metadata = {
                        'name': remote_file,
                        'parents': [drive_folder_id],
                    }
                    service.files().create(supportsAllDrives=True, media_body=media, body=file_metadata).execute()
                    print(f'{remote_file} uploaded to google drive.')

        except HttpError as error:
            sys.exit(f'error checking google drive: {error}')
    else:
        print('skipping...')


def find_or_create_folder(service, folder_name: str, parent_folder_id: str) -> str:  # noqa: ANN001 | service is dynamically typed
    """
    finds or creates the specified folder on the google drive

    args:
        service: an authorized google drive service
        folder_name: the folder name
        parent_folder_id: the id of the parent folder on the google drive

    returns:
        the folder id of the target folder
    """
    try:
        results = service.files().list(q=f"name = '{folder_name}' and '{parent_folder_id}' in parents",
                                       supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
        files = results.get('files', [])
        if not files:
            file_metadata = {
                'name': folder_name,
                'parents': [parent_folder_id],
                'mimeType': 'application/vnd.google-apps.folder',
            }
            folder = service.files().create(supportsAllDrives=True, body=file_metadata).execute()
            return folder['id']
        return files[0]['id']

    except HttpError as error:
        sys.exit(f'error checking google drive: {error}')


def upload_directory(service, sftp: paramiko.SFTPClient, remote_path: str, drive_folder_id: str) -> None:  # noqa: ANN001 | service is dynamically typed
    """
    upload an entire directory from an sftp to the google drive as needed

    args:
        service: an authorized google drive service
        sftp: a connected paramiko SFTPClient
        remote_path: the remote path to the directory
        drive_folder_id: the id for the target google drive folder
    """
    sftp.chdir(remote_path)
    for item in sftp.listdir():
        remote_item_path = remote_path + item
        mode = sftp.lstat(remote_item_path).st_mode
        if not isinstance(mode, int):
            sys.exit(f'could not get mode for {remote_item_path} on sftp')
        if stat.S_ISREG(mode):
            upload_file(service, sftp, remote_item_path, drive_folder_id)
        elif stat.S_ISDIR(mode):
            subfolder_name = item
            subfolder_drive_folder_id = find_or_create_folder(service, subfolder_name, drive_folder_id)
            upload_directory(service, sftp, remote_item_path + '/', subfolder_drive_folder_id)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='backup either the vendor or pmp sftp')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-v', '--vendor', action='store_true', help='backup vendor sftp')
    group.add_argument('-p', '--pmp', action='store_true', help='backup pmp sftp')

    args = parser.parse_args()
    load_dotenv()
    if args.vendor:
        sftp_host = os.environ['SFTP_HOST']
        sftp_port = os.environ['SFTP_PORT']
        sftp_user = os.environ['SFTP_USERNAME']
        sftp_password = os.environ['SFTP_PASSWORD']
        remote_path = os.environ['SFTP_REMOTE_PATH']

        drive_folder_id = os.environ['SFTP_BACKUP_FOLDER']
    elif args.pmp:
        sftp_host = os.environ['PMP_SFTP_HOST']
        sftp_port = os.environ['PMP_SFTP_PORT']
        sftp_user = os.environ['PMP_SFTP_USERNAME']
        sftp_password = os.environ['PMP_SFTP_PASSWORD']
        remote_path = os.environ['PMP_SFTP_REMOTE_PATH']

        drive_folder_id = os.environ['PMP_SFTP_BACKUP_FOLDER']
    else:
        sys.exit('we should never be here')

    creds = auth.auth()
    service = build('drive', 'v3', credentials=creds)
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=sftp_host, port=int(sftp_port), username=sftp_user, password=sftp_password)
    sftp = ssh.open_sftp()

    vendor = "vendor" if args.vendor else "pmp"
    print(f'updating {vendor} sftp backup...')
    upload_directory(service, sftp, remote_path, drive_folder_id)

    sftp.close()
    ssh.close()
    print(f'{vendor} sftp backup complete')
