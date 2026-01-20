import datetime
import os
from typing import TYPE_CHECKING

import polars as pl
from az_pmp_utils import auth, drive
from dotenv import load_dotenv
from googleapiclient.discovery import build

from constants import PHX_TZ

if TYPE_CHECKING:
    import google.auth.external_account_authorized_user
    import google.oauth2.credentials


def pull_files(service) -> pl.DataFrame:  # noqa: ANN001 | service is dynamically typed
    """
    pull the dispensations, ob dispensations, and requests files from the google drive

    args:
        service: an authorized google drive service

    returns:
        scorecard_new_row: the new row for adding to the scorecard tracking sheet
    """
    last_month = datetime.datetime.now(tz=PHX_TZ).date().replace(day=1) - datetime.timedelta(days=1)
    lm_ym = f'{last_month.year}{str(last_month.month).zfill(2)}'
    disp_file_name = f'AZ_Dispensations_{lm_ym}.csv'
    ob_file_name = f'AZ_Dispensations_{lm_ym}_opioid_benzo.csv'

    disp = drive.lazyframe_from_file_name(service=service, file_name=disp_file_name, folder_id=os.environ['DISPENSATIONS_47_FOLDER'], drive_ft='csv', separator='|', infer_schema_length=10000)
    ob_disp = drive.lazyframe_from_file_name(service=service, file_name=ob_file_name, folder_id=os.environ['DISPENSATIONS_47_FOLDER'], drive_ft='csv', separator='|', infer_schema_length=10000)

    patient_req_id = os.environ['PATIENT_REQUESTS_FOLDER']
    requests_folder_id = drive.folder_id_from_name(service=service, folder_name=f'AZ_PtReqByProfile_{lm_ym}', parent_folder_id=patient_req_id)
    requests = drive.lazyframe_from_file_name(service=service, file_name='Prescriber.csv', folder_id=requests_folder_id, drive_ft='csv', separator='|', infer_schema_length=10000)

    def add_lookups(dispensations: pl.LazyFrame, *, ob: bool = False) -> pl.DataFrame:
        """
        adds the lookups and lookup percentage to the dispensations

        args:
            dispensations: the dispensations lazyframe
            ob: whether the dispensations file is the opioid benzo one or not

        returns:
            the row part for the given dispensations lf with `n_prescribers`, `n_lookups`, and `%`"
        """
        lookups = (
            requests
            .select('dea_number', 'totallookups')
        )
        disps = (
            dispensations
            .filter(pl.col('state') == 'AZ')
            .select('dea_number')
            .join(lookups, on='dea_number', how='left')
            .group_by('dea_number')
            .sum()
            .collect()
        )
        n_lookups = disps.filter(pl.col('totallookups') > 0).height
        prefix = 'ob_' if ob else None
        return pl.DataFrame({f'{prefix}n_prescribers': [disps.height], f'{prefix}n_lookups': [n_lookups], f'{prefix}%': [round((n_lookups / disps.height) * 100, 2)]})

    df_lookups = add_lookups(disp)
    ob_df_lookups = add_lookups(ob_disp, ob=True)
    date_col = pl.DataFrame({'date': [last_month.strftime('%b-%y')]})
    return pl.concat([date_col, df_lookups, ob_df_lookups], how='horizontal')


def update_scorecard_sheet(creds: google.oauth2.credentials.Credentials | google.auth.external_account_authorized_user.Credentials, new_row: pl.DataFrame) -> None:
    """
    update the scorecard sheet with the new row

    args:
        creds: google credentials returned by `auth.auth()`
        new_row: the new row returned by `pull_files()`
    """
    sheet_id = os.environ['SCORECARD_FILE']
    service = build('sheets', 'v4', credentials=creds)
    result = service.spreadsheets().values().get(spreadsheetId=sheet_id, range='scorecard!A:A').execute()
    values = result.get('values', [])

    last_row = len(values) if values else 1

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

    creds = auth.auth()
    service = build('drive', 'v3', credentials=creds)

    new_row = pull_files(service)
    update_scorecard_sheet(creds, new_row)
