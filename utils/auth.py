from pathlib import Path

import google.auth.external_account_authorized_user
import google.oauth2.credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow


def auth() -> google.oauth2.credentials.Credentials | google.auth.external_account_authorized_user.Credentials:
    """
    authorization for google services

    returns:
        creds: credentials for building google services
    """
    # if modifying these scopes, delete the file token.json. scopes here: https://developers.google.com/identity/protocols/oauth2/scopes#drive
    scopes = [
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/documents',
        'https://www.googleapis.com/auth/gmail.send',
        'https://www.googleapis.com/auth/gmail.compose',
    ]

    creds = None
    # the file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if Path('token.json').exists():
        creds = google.oauth2.credentials.Credentials.from_authorized_user_file('token.json', scopes)
    # if there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', scopes)
            creds = flow.run_local_server(port=0)
        # save the credentials for the next run
        Path('token.json').write_text(creds.to_json(), encoding='utf-8')
    return creds
