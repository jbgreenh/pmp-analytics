import toml

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from utils.auth import *

def main():
    creds = auth()
    with open('../secrets.toml', 'r') as f:
        secrets = toml.load(f)
    try:
        service = build('drive', 'v3', credentials=creds)

        folder_id = secrets['folders']['pmp_secure']
        query =  f"parents in '{folder_id}' and mimeType = 'application/vnd.google-apps.folder'"
        results = service.files().list(supportsAllDrives=True, includeItemsFromAllDrives=True, q=query, fields = "nextPageToken, files(id, name)").execute()

        items = results.get('files', [])

        if not items:
            print('No files found.')
            return
        print('files in secure:')
        for item in items:
            print(f'{item["name"]} ({item["id"]})')
    except HttpError as error:
        # TODO(developer) - Handle errors from drive API.
        print(f'An error occurred: {error}')


if __name__ == '__main__':
    main()