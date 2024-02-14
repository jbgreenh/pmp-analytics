import paramiko
import toml
import os, sys
from datetime import datetime, timedelta, timezone
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload
from utils.auth import *

def upload_file(service, sftp, remote_file_path, drive_folder_id):
    # check if the file exists in the google drive folder if the file was updated in the last 24 hrs
    file_exists = False
    drive_file_id = None
    remote_file = os.path.basename(remote_file_path)  # extract the filename from the path

    remote_file_mtime = datetime.fromtimestamp(sftp.lstat(remote_file_path).st_mtime).astimezone(timezone.utc)
    current_utc_datetime = datetime.now(timezone.utc)
    time_dif = current_utc_datetime - remote_file_mtime
    if time_dif <= timedelta(hours=24):
        try:
            results = service.files().list(q=f"name = '{remote_file}' and '{drive_folder_id}' in parents",
                                        supportsAllDrives=True, 
                                        includeItemsFromAllDrives=True,
                                        fields='files(id, modifiedTime)').execute()
            files = results.get('files', [])
            if files:
                file_exists = True
                drive_file_id = files[0]['id']
                drive_file_modified_time = datetime.fromisoformat(files[0]['modifiedTime'])
        except HttpError as error:
            print(f'error checking google drive: {error}')

        if not file_exists:
            print(f'uploading {remote_file} to google drive...')
            # sftp -> google drive
            with sftp.file(remote_file_path, 'rb') as remote_file_content:
                remote_file_content.prefetch()
                media = MediaIoBaseUpload(remote_file_content, mimetype='application/octet-stream', chunksize=1024*1024, resumable=True)
                file_metadata = {
                    'name': remote_file,
                    'parents': [drive_folder_id],
                }
                service.files().create(supportsAllDrives=True, media_body=media, body=file_metadata).execute()
                print(f'{remote_file} uploaded to google drive.')
        else:
            # check if the sftp file is newer
            if remote_file_mtime > drive_file_modified_time:
                print(f'updating {remote_file} on google drive...')
                # sftp -> google drive
                with sftp.file(remote_file_path, 'rb') as remote_file_content:
                    remote_file_content.prefetch()
                    media = MediaIoBaseUpload(remote_file_content, mimetype='application/octet-stream', chunksize=1024*1024, resumable=True)
                    service.files().update(fileId=drive_file_id, media_body=media, supportsAllDrives=True).execute()
                    print(f'{remote_file} updated on google drive.')


def upload_directory(service, sftp, remote_path, drive_folder_id):
    sftp.chdir(remote_path)
    for item in sftp.listdir():
        remote_item_path = remote_path + item
        if sftp.lstat(remote_item_path).st_mode & 32768:  # a file
            upload_file(service, sftp, remote_item_path, drive_folder_id)
        elif sftp.lstat(remote_item_path).st_mode & 16384:  # a directory
            subfolder_name = item
            subfolder_drive_folder_id = find_or_create_folder(service, subfolder_name, drive_folder_id)
            upload_directory(service, sftp, remote_item_path + '/', subfolder_drive_folder_id)


def find_or_create_folder(service, name, parent_folder_id):
    # check if the folder already exists in google drive
    folder_exists = False
    folder_id = None
    try:
        results = service.files().list(q=f"name = '{name}' and '{parent_folder_id}' in parents",
                                       supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
        files = results.get('files', [])
        if files:
            folder_exists = True
            folder_id = files[0]['id']
    except HttpError as error:
        print(f'error checking google drive: {error}')

    if not folder_exists:
        # if the folder doesn't exist, create it
        file_metadata = {
            'name': name,
            'parents': [parent_folder_id],
            'mimeType': 'application/vnd.google-apps.folder',
        }
        folder = service.files().create(supportsAllDrives=True, body=file_metadata).execute()
        return folder['id']
    else:
        return folder_id
    

if __name__ == '__main__':
    with open('../secrets.toml', 'r') as f:
        secrets = toml.load(f)

    if len(sys.argv) != 2 or (sys.argv[1] not in ['vendor', 'pmp']):
        print('please run with one of the below formats')
        print('python sftp_backup.py vendor')
        print('python sftp_backup.py pmp')
        sys.exit(0) 

    version = sys.argv[1]
    if version == 'vendor':
        sftp_host = secrets['sftp']['host']
        sftp_port = secrets['sftp']['port']
        sftp_user = secrets['sftp']['username']
        sftp_password = secrets['sftp']['password']
        remote_path = secrets['sftp']['remote_path']

        drive_folder_id = secrets['folders']['sftp_backup']
    else:
        sftp_host = secrets['pmp_sftp']['host']
        sftp_port = secrets['pmp_sftp']['port']
        sftp_user = secrets['pmp_sftp']['username']
        sftp_password = secrets['pmp_sftp']['password']
        remote_path = secrets['pmp_sftp']['remote_path']

        drive_folder_id = secrets['folders']['pmp_sftp_backup']

    creds = auth()
    service = build('drive', 'v3', credentials=creds)
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=sftp_host, port=sftp_port, username=sftp_user, password=sftp_password)
    sftp = ssh.open_sftp()  
    print(f'updating {version} sftp backup...') 
    upload_directory(service, sftp, remote_path, drive_folder_id)

    sftp.close()
    ssh.close()
    print(f'{version} sftp backup complete')