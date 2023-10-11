import polars as pl
import os
import toml

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from utils.auth import *

def main():
    creds = auth()
    with open('../secrets.toml', 'r') as f:
        secrets = toml.load(f)
    try:
        service = build('drive', 'v3', credentials=creds)

        folder_id = secrets['folders']['pharm_clean']
        
        file_metadata = {
            'name': 'pizza.csv', 
            'parents':[folder_id],
            'mimeType': 'application/vnd.google-apps.spreadsheet'
        }
        pl.DataFrame({'pizza':'yes'}).write_csv('pizza.csv')

        media = MediaFileUpload('pizza.csv',
                                mimetype='text/csv')
        file = service.files().create(body=file_metadata,
                                      media_body=media,
                                      supportsAllDrives=True,
                                      fields='id').execute()
        print (f'file ID: {file.get("id")}')
        
    except HttpError as error:
        # TODO(developer) - Handle errors from drive API.
        print(f'An error occurred: {error}')

if __name__ == '__main__':
    main()
    os.remove('pizza.csv')