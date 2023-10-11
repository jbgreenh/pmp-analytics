import polars as pl
import io
import toml

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from utils.auth import *

def main():
    creds = auth()
    with open('secrets.toml', 'r') as f:
        secrets = toml.load(f)
    try:
        service = build('drive', 'v3', credentials=creds)

        file_id = secrets['files']['awarxe']
        service = build('drive', 'v3', credentials=creds)
        request = service.files().get_media(fileId=file_id)
        file = io.BytesIO()
        downloader = MediaIoBaseDownload(file, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print("Download %d%%." % int(status.progress() * 100))
    except HttpError as error:
        # TODO(developer) - Handle errors from drive API.
        print(f'An error occurred: {error}')
        file = None

    file.seek(0) # after writing, pointer is at the end of the stream
    print(pl.read_csv(file, separator='|', infer_schema_length=100000))

if __name__ == '__main__':
    main()