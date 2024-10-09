import os
from datetime import date, timedelta

import paramiko
import toml
from googleapiclient.discovery import build

from utils import auth, drive


def get_last_sunday() -> date:
    """
    gets the date of the last sunday

    returns:
        a datetime.date for the last sunday
    """
    today = date.today()
    days_since_sunday = today.weekday() + 1
    return today - timedelta(days=days_since_sunday)

def remove_oldest_file(sftp:paramiko.SFTPClient):
    """
    removes the oldest file from the current folder in the sftp

    args:
        `sftp`: paramiko SFTPClient
    """
    files = sftp.listdir_attr()
    oldest_file = min(files, key=lambda f: f.st_mtime)
    print(f'removing oldest file: {oldest_file.filename}...')
    sftp.remove(oldest_file.filename)
    print('file removed')

def upload_latest_dhs_file(service, sftp:paramiko.SFTPClient, folder:str):
    """
    uploads the latest standard extract to the DHS sftp

    args:
        `service`: google drive service
        `sftp`: paramiko SFTPClient connected to the DHS sftp
        `folder`: the google drive folder for the standard extracts
    """
    last_sunday = get_last_sunday()
    file_name = last_sunday.strftime('AZ_%Y%m%d.csv')
    extract = drive.lazyframe_from_file_name_csv(service, file_name=file_name, folder_id=folder, separator='|', infer_schema_length=100000)
    extract.collect().write_csv(file_name)
    print(f'writing {file_name} to sftp...')
    sftp.put(localpath=file_name, remotepath=file_name)
    print('file uploaded')
    os.remove(file_name)

def main():
    creds = auth.auth()
    service = build('drive', 'v3', credentials=creds)

    with open('../secrets.toml', 'r') as f:
        secrets = toml.load(f)

    folder = secrets['folders']['standard_extract']

    sftp_host = secrets['servu']['host']
    sftp_port = secrets['servu']['port']
    sftp_user = secrets['servu']['username']
    sftp_password = secrets['servu']['password']
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=sftp_host, port=sftp_port, username=sftp_user, password=sftp_password)
    sftp = ssh.open_sftp()

    upload_latest_dhs_file(service, sftp, folder)
    remove_oldest_file(sftp)

    sftp.close()
    ssh.close()

if __name__ == '__main__':
    main()
