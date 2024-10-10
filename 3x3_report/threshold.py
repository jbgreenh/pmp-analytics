import polars as pl
import datetime
import toml
import sys
from googleapiclient.discovery import build

from utils import auth

def threshold_report(patient_number:int):
    """
    takes the number of patients and prints the date, patinet number, successful and failed email attempts

    args:
        patient_number: the number of patients from awarxe
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

    success = thresh_file.shape[0] - failed_to_send.shape[0]
    print(f'{success = }')
    print(f'fail = {failed_to_send.shape[0]}')
    print(thresh_file.shape[0])

    today = datetime.date.today()
    first = today.replace(day=1)
    last_month_date = first - datetime.timedelta(days=1)
    date_str = last_month_date.strftime('%y-%B')
    perc = round(((success / (success + failed_to_send.shape[0])) * 100), 2)
    perc = f'{perc}%'
    out = [date_str, patient_number, success, failed_to_send.shape[0], perc]
    print(out)
    return out

def update_sheet(creds, thresh, file_id:str):
    """
    updates the threshold google sheet    

    args:
        creds: credentials from `auth.auth()`
        thresh: a list containing the row for adding to the end of the 3x3 threshold list
        file_id: the file id of the 3x3 threshold sheet
    """
    range_name = '3x3!A:A'
    service = build('sheets', 'v4', credentials=creds)
    result = service.spreadsheets().values().get(spreadsheetId=threshold_sheet_id, range=range_name).execute()
    values = result.get('values', [])
    last_row = len(values) if values else 1
    data_range = f'3x3!A{last_row + 1}:E{last_row + 1}'
    request = service.spreadsheets().values().update(
        spreadsheetId=file_id,
        range=data_range,
        valueInputOption='RAW',
        body={'values': [thresh]}
    )
    _response = request.execute()
    print(f'3x3 Threshold sheet is updated at https://docs.google.com/spreadsheets/d/{file_id}')

def main():
    if len(sys.argv) != 2 or not (sys.argv[1].isdigit()):
        sys.exit('no')
    else:
        threshold_report(int(sys.argv[1]))

if __name__ == '__main__':
    with open('../secrets.toml', 'r') as f:
        secrets = toml.load(f)
    threshold_sheet_id = secrets['files']['threshold_sheet']
    threshold_sheet_folder = secrets['folders']['threshold_reports']

    creds = auth.auth()
    service = build('drive', 'v3', credentials=creds)

    thresh = threshold_report(sys.argv[1])
    updating = update_sheet(creds, thresh, threshold_sheet_id)
