import os
import polars as pl
import datetime
from dotenv import load_dotenv

from googleapiclient.discovery import build
from utils import auth, drive

def pull_files(service, last_month):
    '''
    pull the proper dispensations and request files
    '''
    lm_yr = last_month.year
    lm_mo = str(last_month.month).zfill(2)
    file_name = f'AZ_Dispensations_{lm_yr}{lm_mo}.csv'
    ob_file_name = f'AZ_Dispensations_{lm_yr}{lm_mo}_opioid_benzo.csv'

    folder_id = os.environ.get('DISPENSATIONS_47_FOLDER')
    disp = drive.lazyframe_from_file_name(service=service, file_name=file_name, folder_id=folder_id, drive_ft='csv', separator='|', infer_schema_length=10000)
    ob_disp = drive.lazyframe_from_file_name(service=service, file_name=ob_file_name, folder_id=folder_id, drive_ft='csv', separator='|', infer_schema_length=10000)

    patient_req_id = os.environ.get('PATIENT_REQUESTS_FOLDER')
    requests_folder_id = drive.folder_id_from_name(service=service, folder_name=f'AZ_PtReqByProfile_{lm_yr}{lm_mo}', parent_id=patient_req_id)
    requests = drive.lazyframe_from_file_name(service=service, file_name='Prescriber.csv', folder_id=requests_folder_id, drive_ft='csv', separator='|', infer_schema_length=10000)

    return disp, ob_disp, requests


def scorecard_new_row(service, last_month):
    disp, ob_disp, requests = pull_files(service, last_month)

    lookups = (
        requests
        .select('dea_number','totallookups')
    )

    disps = (
        disp
        .filter(pl.col('state') == 'AZ')
        .select('dea_number')
        .join(lookups, on='dea_number', how='left', coalesce=True)
        .group_by('dea_number')
        .sum()
        .collect()
    )

    n = disps.height
    n_lookups = disps.filter(pl.col('totallookups') > 0).height
    n_lookups_per = (n_lookups / n) * 100
    n_lookups_str = f'{round(n_lookups_per, 2)}'
    df_lookups = pl.DataFrame({'n_prescribers': [n], 'n_lookups': [n_lookups], '%': [float(n_lookups_str)]})

    ob_disps = (
        ob_disp
        .filter(pl.col('state') == 'AZ')
        .select('dea_number')
        .join(lookups, on='dea_number', how='left', coalesce=True)
        .group_by('dea_number')
        .sum()
        .collect()
    )

    ob_n = ob_disps.height
    ob_n_lookups = ob_disps.filter(pl.col('totallookups') > 0).height
    ob_n_lookups_per = (ob_n_lookups / ob_n) * 100
    ob_n_lookups_str = f'{round(ob_n_lookups_per, 2)}'
    ob_df_lookups = pl.DataFrame({'ob_n_prescribers': [ob_n], 'ob_n_lookups': [ob_n_lookups], 'ob_%': [float(ob_n_lookups_str)]})

    m_y_str = last_month.strftime('%b-%y')
    date_col = pl.DataFrame({'date': [m_y_str]})
    combined = pl.concat([date_col, df_lookups, ob_df_lookups], how='horizontal')
    return combined


def update_scorecard_sheet(creds, new_row):
    sheet_id = os.environ.get('SCORECARD_FILE')
    range_name = 'scorecard!A:A'
    service = build('sheets', 'v4', credentials=creds)
    result = service.spreadsheets().values().get(spreadsheetId=sheet_id, range=range_name).execute()
    values = result.get('values', [])

    if values:
        last_row = len(values)
    else:
        last_row = 1

    data = [list(row) for row in new_row.rows()]
    data_range = f'scorecard!A{last_row + 1}:{chr(65 + len(data[0]))}{last_row + len(data) + 1}'

    request = service.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=data_range,
        valueInputOption='RAW',
        body={'values': data}
    )
    _response = request.execute()
    sheet_link = f'https://docs.google.com/spreadsheets/d/{sheet_id}'
    print(f'updated scorecard tracking: {sheet_link}')


if __name__ == '__main__':
    load_dotenv()

    today = datetime.datetime.now()
    last_month = today.replace(day=1) - datetime.timedelta(days=1)

    creds = auth.auth()
    service = build('drive', 'v3', credentials=creds)

    new_row = scorecard_new_row(service, last_month)
    update_scorecard_sheet(creds, new_row)
