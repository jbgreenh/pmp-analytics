import os
from datetime import date, datetime

import polars as pl
from dotenv import load_dotenv
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

load_dotenv()
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


def test_lazyframe_from_filename() -> None:
    """test the lazyframe_from_filename function"""
    test_folder = os.environ['TEST_FOLDER']
    test_sheet = drive.lazyframe_from_file_name(service, 'test sheet', folder_id=test_folder, drive_ft='sheet')
    assert isinstance(test_sheet, pl.LazyFrame)
    test_sheet_df = test_sheet.collect()
    assert test_sheet_df['a'].first() == 1
    assert test_sheet_df['a'].sum() == 6
    assert test_sheet_df['a'].last() == 2
    assert test_sheet_df['b'].first() == 2
    assert test_sheet_df['b'].last() == 3
    assert test_sheet_df['b'].sum() == 7
    assert test_sheet_df['c'].first() == 3
    assert test_sheet_df['c'].last() == 1
    assert test_sheet_df['c'].sum() == 5

    test_csv = drive.lazyframe_from_file_name(service, 'test_csv.csv', folder_id=test_folder, drive_ft='csv')
    assert isinstance(test_csv, pl.LazyFrame)
    test_csv_df = test_csv.collect()
    assert test_csv_df['a'].first() == 1
    assert test_csv_df['a'].sum() == 6
    assert test_csv_df['a'].last() == 2
    assert test_csv_df['b'].first() == 2
    assert test_csv_df['b'].last() == 3
    assert test_csv_df['b'].sum() == 7
    assert test_csv_df['c'].first() == 3
    assert test_csv_df['c'].last() == 1
    assert test_csv_df['c'].sum() == 5


def test_get_latest_upload() -> None:
    """test the get_latest_upload function"""
    latest_file = drive.get_latest_uploaded(service, folder_id=os.environ['TEST_UPLOADS_FOLDER'], drive_ft='sheet')
    assert isinstance(latest_file.created_at, datetime)
    assert latest_file.created_at.month == 9
    assert latest_file.created_at.day == 23
    assert latest_file.created_at.year == 2025

    assert isinstance(latest_file.lf, pl.LazyFrame)
    latest_df = latest_file.lf.collect()
    assert latest_df.columns == ['new']
    assert latest_df['new'].first() == 2
