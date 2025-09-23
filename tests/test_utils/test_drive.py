import os
from datetime import date, datetime
from pathlib import Path

import polars as pl
import pytest
from dotenv import load_dotenv
from googleapiclient.discovery import build

from utils import auth, drive

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

    with pytest.raises(drive.GoogleDriveNotFoundError):
        _fake_file = drive.lazyframe_from_file_name(service, 'this_file_doesnt_exist', folder_id=test_folder, drive_ft='sheet')


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


def test_folder_id_from_name() -> None:
    """test folder_id_from_name function"""
    folder_id = drive.folder_id_from_name(service, folder_name='test uploads', parent_folder_id=os.environ['TEST_FOLDER'])
    assert folder_id == os.environ['TEST_UPLOADS_FOLDER']

    make_folder = drive.folder_id_from_name(service, folder_name='make folder', parent_folder_id=os.environ['TEST_FOLDER'], create=True)
    assert make_folder == drive.folder_id_from_name(service, folder_name='make folder', parent_folder_id=os.environ['TEST_FOLDER'])

    _response = service.files().delete(fileId=make_folder, supportsAllDrives=True).execute()
    with pytest.raises(drive.GoogleDriveNotFoundError):
        _fake_folder = drive.folder_id_from_name(service, folder_name='make folder', parent_folder_id=os.environ['TEST_FOLDER'])


def test_upload_csv_as_sheet() -> None:
    """test upload_csv_as_sheet function"""
    test_df = pl.DataFrame({
        'a': [1, 1, 1, 6],
        'b': [2, 2, 2, 9],
    })
    file_path = Path('data/test_df_upload.csv')
    test_df.write_csv(file_path)
    test_folder = os.environ['TEST_FOLDER']
    drive.upload_csv_as_sheet(service, file_path=file_path, folder_id=test_folder)
    file_path.unlink()
    upload_csv = drive.lazyframe_from_file_name(service, file_name='test_df_upload', folder_id=test_folder, drive_ft='sheet').collect()

    assert test_df.schema == upload_csv.schema
    assert upload_csv['a'].sum() == 9
    assert upload_csv['a'].first() == 1
    assert upload_csv['a'].last() == 6
    assert upload_csv['b'].sum() == 15
    assert upload_csv['b'].first() == 2
    assert upload_csv['b'].last() == 9

    results = service.files().list(q=f"name = 'test_df_upload' and '{test_folder}' in parents", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
    files = results.get('files', [])
    _response = service.files().delete(fileId=files[0]['id'], supportsAllDrives=True).execute()


def test_update_sheet() -> None:
    """test update_sheet function"""
    test_df = pl.DataFrame({
        'd': [1, 2, 3],
        'e': [3, 2, 1],
        'f': [2, 3, 1],
    })
    fp = Path('data/test_update.csv')
    test_df.write_csv(fp)
    drive.update_sheet(service, fp, file_id=os.environ['TEST_SHEET'])
    test_sheet = drive.lazyframe_from_file_name(service, file_name='test sheet', folder_id=os.environ['TEST_FOLDER'], drive_ft='sheet').collect()
    assert test_sheet['d'].first() == 1
    assert test_sheet['d'].last() == 3
    assert test_sheet['d'].sum() == 6
    assert test_sheet['e'].first() == 3
    assert test_sheet['e'].last() == 1
    assert test_sheet['e'].sum() == 6
    assert test_sheet['f'].first() == 2
    assert test_sheet['f'].last() == 1
    assert test_sheet['f'].sum() == 6

    # restore to original
    test_df = pl.DataFrame({
        'a': [1, 3, 2],
        'b': [2, 2, 3],
        'c': [3, 1, 1],
    })
    test_df.write_csv(fp)
    drive.update_sheet(service, fp, file_id=os.environ['TEST_SHEET'])
    test_sheet = drive.lazyframe_from_file_name(service, file_name='test sheet', folder_id=os.environ['TEST_FOLDER'], drive_ft='sheet').collect()
    assert test_sheet['a'].first() == 1
    assert test_sheet['a'].sum() == 6
    assert test_sheet['a'].last() == 2
    assert test_sheet['b'].first() == 2
    assert test_sheet['b'].last() == 3
    assert test_sheet['b'].sum() == 7
    assert test_sheet['c'].first() == 3
    assert test_sheet['c'].last() == 1
    assert test_sheet['c'].sum() == 5
    fp.unlink()


def test_lazyframe_from_id_and_sheetname() -> None:
    """test lazyframe_from_id_and_sheetname function"""
    test_sheet = drive.lazyframe_from_id_and_sheetname(service, file_id=os.environ['TEST_SHEET'], sheet_name='test sheet')
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
