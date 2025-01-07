import polars as pl
import datetime
import toml
import sys
from dataclasses import dataclass
from googleapiclient.discovery import build

from utils import auth

@dataclass
class ThresholdInfo:
    """
    class with everything needed to update the 3x3 threshold report

    attributes: 
        `date_str`: a `str` for the date of the report in %y-%B format
        `patient_number`: an `int` with the number of patients on the 3x3 list
        `success`: an `int` with the number of emails succesfully sent by the vendor
        `failed_to_send`: an `int` with the number of emails that failed to send
        `perc`: a `str` with the percent of succesful emails sent: (success/(success + failed_to_send)) * 100
    """
    date_str: str
    patient_number: int
    success: int
    failed_to_send: int
    perc: str

def threshold_report(patient_number:int) -> ThresholdInfo:
    """
    takes the number of patients and prints the date, patinet number, successful and failed email attempts

    args:
        `patient_number`: the number of patients from awarxe

    returns:
        `update_row`: a `ThresholdInfo` which includes all the information for adding a new row to the 3x3 report
    """

    thresh_file = (
        pl.read_csv('data/AZ 3x3.csv')
        .with_columns(
            pl.col('Email').fill_null(pl.lit('EMPTY'))
        )
    )

    bad_emails = ['0', 'EMPTY', 'NONE@EMAIL.COM', 'NONE@NONE.COM', 'NONE@GMAIL.COM']

    failed_to_send = (
        thresh_file
        .filter(
            pl.col('Email').str.to_uppercase().is_in(bad_emails)
        )
    )

    success = thresh_file.height - failed_to_send.height
    print(f'{success = }')
    print(f'fail = {failed_to_send.height}')
    print(thresh_file.height)

    today = datetime.date.today()
    first = today.replace(day=1)
    last_month_date = first - datetime.timedelta(days=1)
    date_str = last_month_date.strftime('%y-%B')
    perc = round(((success / (success + failed_to_send.height)) * 100), 2)
    perc = f'{perc}%'
    update_row = ThresholdInfo(date_str, patient_number, success, failed_to_send.height, perc)
    return update_row

def update_sheet(creds, thresh:ThresholdInfo, file_id:str):
    """
    updates the threshold google sheet with the '3x3' list

    args:
        creds: credentials from `auth.auth()`
        thresh: the `ThresholdInfo` returned by `threshold_report()`
        file_id: the google drive file id of the 3x3 threshold sheet
    """
    range_name = '3x3!A:A'
    service = build('sheets', 'v4', credentials=creds)
    result = service.spreadsheets().values().get(spreadsheetId=file_id, range=range_name).execute()
    values = result.get('values', [])
    last_row = len(values) if values else 1
    data_range = f'3x3!A{last_row + 1}:E{last_row + 1}'
    request = service.spreadsheets().values().update(
        spreadsheetId=file_id,
        range=data_range,
        valueInputOption='RAW',
        body={'values': [[thresh.date_str, thresh.patient_number, thresh.success, thresh.failed_to_send, thresh.perc]]}
    )
    _response = request.execute()
    print(f'3x3 Threshold sheet is updated at https://docs.google.com/spreadsheets/d/{file_id}')

def main():
    if len(sys.argv) != 2 or not (sys.argv[1].isdigit()):
        print('please insert the number of patients from the threshold request in awarxe')
        sys.exit('eg: python threshold.py 678')
    else:
        with open('../secrets.toml', 'r') as f:
            secrets = toml.load(f)

        threshold_sheet_id = secrets['files']['threshold_sheet']

        creds = auth.auth()
        thresh = threshold_report(int(sys.argv[1]))
        update_sheet(creds, thresh, threshold_sheet_id)

if __name__ == '__main__':
    main()
