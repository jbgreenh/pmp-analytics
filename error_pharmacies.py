import os
import pathlib
from typing import Any

import polars as pl
from dotenv import load_dotenv
from googleapiclient.discovery import build

from utils import auth, drive, tableau


def pull_file() -> pl.LazyFrame:
    """
    pulls the file from tableau and returns a lazyframe of the pharmacy with the most errors error information

    returns:
        top_pharmacy: a LazyFrame containing the top pharmacies errors
    """
    print('pulling error file from tableau...')
    luid = tableau.find_view_luid(view_name="Errors by Pharmacy", workbook_name="Pharmacy Compliance")
    errors_by_pharmacy = (
        tableau.lazyframe_from_view_id(view_id=luid, infer_schema_length=10000).drop("blank")
        .rename({
            'DEA Number': 'dea', 'Pharmacy Name': 'pharmacy', 'License Number': 'license', 'File Name': 'file_name', 'Dispensation ID': 'dispensation_ID', 'Error': 'error',
            'Submission Date': 'submission_date', 'RX Number': 'rx_number', 'Pharmacist Phone': 'pharm_phone',
            'Pharmacist Email': 'pharm_email', 'Written At': 'written', 'Filled At': 'filled', 'Sold At': 'sold', 'Refill Number': 'refills',
            'Days Supply': 'supply', 'Outstanding Age in Days': 'outstanding_days', 'Quantity': 'quantity'
        })
    )

    dea_count = (
        errors_by_pharmacy
        .group_by(['dea', 'pharmacy']).len()
        .sort(by='len', descending=True)
    )
    print('top pharmacies:')
    print(dea_count.collect().head())

    maxdea_count = dea_count.filter(pl.col('len') == pl.col('len').max()).select(pl.col('dea')).collect().item()
    print(errors_by_pharmacy.collect().head())
    top_dea = (
        errors_by_pharmacy
        .filter(pl.col('dea') == maxdea_count)
        .with_columns(
            pl.col('submission_date').str.to_datetime('%-m/%-d/%Y %-l:%-M:%-S %p')
        )

    )

    top_pharmacy_name = top_dea.select(pl.col('pharmacy')).head(1).collect().item()
    file_name = top_pharmacy_name
    top_dea.collect().write_csv(f'{file_name}.csv')
    print(f'{file_name} written locally')
    return top_dea


def row_for_sheet(top_pharmacy: pl.LazyFrame, folder_id: str) -> list[Any]:
    """
    takes the lazyframe and retrieves the rows as a list with organized columns to prepare it before updating the google sheet

    args:
        top_pharmacy: the LazyFrame folder_id: the folder id for the current top pharmacy returned by `find_or_create_folder()`
        folder_id: a string with the google drive id for the parent folder

    returns:
        returns the rows as a list in an organized fashion to match the columns on the google sheet
    """
    igov = (
        pl.scan_csv('data/List Request.csv', infer_schema_length=0)
        .filter(
            pl.col('Type') == 'Pharmacy'
        )
        .with_columns(
            pl.col('License/Permit #').str.to_uppercase().str.strip_chars().alias('license'),
            pl.concat_str(
                    [
                        pl.col('Street Address'),
                        pl.col('Apt/Suite #')
                    ],
                    separator=' '
                ).alias('Address'),
            pl.concat_str(
                    [
                        pl.col('City'),
                        pl.lit(',')
                    ]
                ).alias('City,')
            )
        .with_columns(
            pl.concat_str(
                    [
                        pl.col('City,'),
                        pl.col('State'),
                        pl.col('Zip')
                    ],
                    separator=' '
                ).alias('CSZ')
            )
            .select(
                'license', 'Business Name', 'Address', 'CSZ', 'Email', 'Phone'
            )
        )

    folder_url = f"https://drive.google.com/drive/folders/{folder_id}"

    return list(
        top_pharmacy
        .join(igov, on='license', how='left')
        .with_columns(
            pl.col('submission_date').min().dt.to_string('%-m/%-d/%Y %-l:%-M:%-S %p').alias('error_start_date'),
            pl.col('submission_date').max().dt.to_string('%-m/%-d/%Y %-l:%-M:%-S %p').alias('error_end_date'),
            pl.col('error').len().alias('num_of_errors'),
            pl.lit(folder_url).alias('folder_link')
        )
        .select('folder_link', 'Business Name', 'Address', 'CSZ', 'license', 'dea', 'Phone', 'Email', 'error_start_date', 'error_end_date', 'num_of_errors')
        .collect()
        .row(1)
    )


def update_error_sheet(creds, row_for_updating: list[Any], file_id: str) -> None:
    """
    adds a given row to the end of the pharmacy error sheet

    args:
        creds: credentials from `auth.auth()`
        row_for_updating: a list containing the row for adding to the end of the pharmacy error sheet
        file_id: the file id of the pharmacy error sheet
    """
    print('getting error sheet from drive...')
    range_name = 'errors!A:A'
    service = build('sheets', 'v4', credentials=creds)
    result = service.spreadsheets().values().get(spreadsheetId=error_sheet_id, range=range_name).execute()
    values = result.get('values', [])
    last_row = len(values) if values else 1
    data_range = f'errors!A{last_row + 1}:K{last_row + 1}'
    request = service.spreadsheets().values().update(
        spreadsheetId=file_id,
        range=data_range,
        valueInputOption='RAW',
        body={'values': [row_for_updating]}
    )
    _response = request.execute()
    print(f'updated pharmacy error sheet at {row_for_updating[0]}')


if __name__ == '__main__':
    load_dotenv()

    error_sheet_id = os.environ['PHARMACY_CORRECTIONS_FILE']
    error_folder_id = os.environ['ERROR_CORRECTIONS_FOLDER']

    creds = auth.auth()
    service = build('drive', 'v3', credentials=creds)
    top_pharmacy = pull_file()
    top_pharmacy_name = (top_pharmacy.select(pl.col('pharmacy')).collect().head(1).item())
    folder_id = drive.find_or_create_folder(service, top_pharmacy_name, error_folder_id)
    file_name = f'{top_pharmacy_name}.csv'
    drive.upload_csv_as_sheet(service, file_name, folder_id)
    pathlib.Path(file_name).unlink()
    print(f'{file_name} removed')
    row_for_updating = row_for_sheet(top_pharmacy, folder_id)
    update_error_sheet(creds, row_for_updating, error_sheet_id)
