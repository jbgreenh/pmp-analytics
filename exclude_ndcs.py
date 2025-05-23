import os

import polars as pl
from utils import tableau, auth, drive

from dotenv import load_dotenv
from googleapiclient.discovery import build

creds = auth.auth()
service = build('drive', 'v3', credentials=creds)

load_dotenv()

sheet_id = os.environ.get('EXCLUDED_NDCS_FILE')

excluded_ndcs = drive.lazyframe_from_id_and_sheetname(service, sheet_id, 'excluded', infer_schema_length=0)

luid = tableau.find_view_luid('opiate_antagonists', 'opiate antagonists')
antagonists = (
    tableau.lazyframe_from_view_id(luid, infer_schema_length=0)
    .join(excluded_ndcs, on='NDC', how='anti')
    .rename(
        {'Generic Name':'drug'}
    )
    .select('NDC', 'drug')
)

new_ndcs = antagonists.collect()

if new_ndcs.is_empty():
    print('no new ndcs found')
else:
    print('please input exclusion list in awarxe')
    print(new_ndcs)
    new_fn = 'new_ndcs.csv'
    new_ndcs.write_csv(new_fn)
    print(f'{new_fn} written')


    new_file = pl.concat([excluded_ndcs.collect(), new_ndcs])

    range_name = 'excluded!A:B'
    service = build('sheets', 'v4', credentials=creds)
    service.spreadsheets().values().clear(spreadsheetId=sheet_id,range=range_name).execute()
    data = [list(row) for row in new_file.rows()]
    data.insert(0, ['NDC', 'drug'])
    body = {
        'values': data
    }
    result = service.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=range_name,
        valueInputOption='RAW',
        body=body
    ).execute()
    print(f"{result.get('updatedCells')} cells updated.")
