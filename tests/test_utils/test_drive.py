from datetime import date

import polars as pl
from googleapiclient.discovery import build

from utils import auth, drive

# TODO: create test folder with test files on drive that never change for checking

AWARXE_SCHEMA = pl.Schema({
    'email address': str,
    'dea number': str,
    'dea suffix': str,
    'npi number': str,
    'first name': str,
    'last name': str,
    'professional license number': str,
    'professional license type': str,
    'address 1': str,
    'address 2': str,
    'city': str,
    'state': str,
    'zip code': str,
    'role category': str,
    'role title': str,
    'registration review date': str
})

creds = auth.auth()
service = build('drive', 'v3', credentials=creds)


def test_awarxe() -> None:
    """test awarxe function"""
    awarxe_24_nye = drive.awarxe(service, day=date(year=2024, month=12, day=31))
    assert isinstance(awarxe_24_nye, pl.LazyFrame)
    awarxe_24_nye_df = awarxe_24_nye.collect()
    assert awarxe_24_nye_df.schema == AWARXE_SCHEMA
    assert awarxe_24_nye_df.height == 72_568

    awarxe = drive.awarxe(service)
    assert isinstance(awarxe, pl.LazyFrame)
    assert awarxe.collect_schema() == AWARXE_SCHEMA
