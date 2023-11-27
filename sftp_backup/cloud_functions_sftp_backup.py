import base64
import paramiko
import os, logging
from io import StringIO
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload
import google.auth
from google.cloud import pubsub_v1


# global log stream; log_stream.getvalue() will have the results
log_stream = StringIO()
logging.basicConfig(stream=log_stream, level=logging.INFO)
today = datetime.now().astimezone(ZoneInfo('America/Phoenix'))
today_str = today.strftime('%m/%d/%Y, %H:%M:%S')
logging.info(f'daily sftp backup begun {today_str}')


def upload_file(service, sftp, remote_file_path, drive_folder_id):
    # check if the file exists in the google drive folder if the file was updated in the last 24 hrs
    file_exists = False
    drive_file_id = None
    remote_file = os.path.basename(remote_file_path)  # extract the filename from the path

    remote_file_mtime = datetime.fromtimestamp(sftp.lstat(remote_file_path).st_mtime).replace(tzinfo=ZoneInfo('America/Phoenix')).astimezone(timezone.utc)
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
            logging.error(f'error checking google drive: {error}')

        if not file_exists:
            logging.info(f'uploading {remote_file} to google drive...')
            # sftp -> google drive
            with sftp.file(remote_file_path, 'rb') as remote_file_content:
                remote_file_content.prefetch()
                media = MediaIoBaseUpload(remote_file_content, mimetype='application/octet-stream', chunksize=1024*1024, resumable=True)
                file_metadata = {
                    'name': remote_file,
                    'parents': [drive_folder_id],
                }
                service.files().create(supportsAllDrives=True, media_body=media, body=file_metadata).execute()
                logging.info(f'{remote_file} uploaded to google drive.')
        else:
            # check if the sftp file is newer
            if remote_file_mtime > drive_file_modified_time:
                print(f'updating {remote_file} on google drive...')
                # sftp -> google drive
                with sftp.file(remote_file_path, 'rb') as remote_file_content:
                    remote_file_content.prefetch()
                    media = MediaIoBaseUpload(remote_file_content, mimetype='application/octet-stream', chunksize=1024*1024, resumable=True)
                    service.files().update(fileId=drive_file_id, media_body=media, supportsAllDrives=True).execute()
                    logging.info(f'{remote_file} updated on google drive.')


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
        logging.error(f'error checking google drive: {error}')

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
    

def backup_sftp():
    sftp_host = os.environ.get('host', 'environment variable is not set')
    sftp_port = os.environ.get('port', 'environment variable is not set')
    sftp_user = os.environ.get('user', 'environment variable is not set')
    sftp_password = os.environ.get('password', 'environment variable is not set')
    remote_path = os.environ.get('path', 'environment variable is not set')

    drive_folder_id = os.environ.get('folder', 'environment variable is not set')

    # secret_file = os.path.join(os.getcwd(), 'service_key.json')
    
    # SCOPES = ['https://www.googleapis.com/auth/drive']

    # creds = service_account.Credentials.from_service_account_file(secret_file, scopes=SCOPES)

    creds, _proj_id = google.auth.default()
    service = build('drive', 'v3', credentials=creds)

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=sftp_host, port=sftp_port, username=sftp_user, password=sftp_password)
    sftp = ssh.open_sftp()   
    upload_directory(service, sftp, remote_path, drive_folder_id)

    sftp.close()
    ssh.close()


def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print(pubsub_message)
    
    backup_sftp()

    today = datetime.now().astimezone(ZoneInfo('America/Phoenix'))
    today_str = today.strftime('%m/%d/%Y, %H:%M:%S')
    logging.info(f'daily sftp backup complete {today_str}')
    
    project_id = os.environ.get('project_id', 'environment variable is not set')
    topic_id = os.environ.get('topic_id', 'environment variable is not set')
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    data = log_stream.getvalue().encode('utf-8')
    future = publisher.publish(topic_path, data)
    print(future.result())
    print('published message')