from pathlib import Path

import google.auth.external_account_authorized_user
import google.oauth2.credentials

from utils import auth


def test_auth() -> None:
    """test the auth util"""
    assert Path('token.json').exists()  # created after first login
    assert Path('credentials.json').exists()  # from gcp
    creds = auth.auth()
    assert isinstance(creds, google.oauth2.credentials.Credentials | google.auth.external_account_authorized_user.Credentials)
