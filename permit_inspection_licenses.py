import os
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
from az_pmp_utils import drive
from dotenv import load_dotenv

from constants import PHX_TZ

load_dotenv()

today = datetime.now(tz=PHX_TZ)
last_mo = today.replace(day=1) - timedelta(days=1)
last_last_mo = (last_mo.replace(day=1) - timedelta(days=1)).replace(day=1)

inspection_tracker_file_id = os.environ['PERMIT_INSPECTION_TRACKER_FILE']
inspections = (
    drive.lazyframe_from_id_and_sheetname(file_id=inspection_tracker_file_id, sheet_name='input', infer_schema_length=0, read_options={'header_row': 4})  # read_excel() does not have infer_schema
    .select(
        pl.col('Current Routine Inspection Date').str.to_date('%Y-%m-%d %H:%M:%S').alias('inspection_date'),
        pl.col('Permit #').str.strip_chars().str.to_uppercase().alias('permit_number'),
        pl.col('DEA Registration Number').str.strip_chars().str.to_uppercase().alias('dea_number'),
        pl.col('Status').str.strip_chars().str.to_uppercase().alias('status'),
        pl.col('Routine Inspection Type').str.to_uppercase().alias('inspect_type'),
        pl.col('Assigned CO').alias('co')
    )
    .filter(
        pl.col('inspection_date') >= last_last_mo,
        pl.col('permit_number').str.starts_with('Y'),
        pl.col('status').str.starts_with('OPEN'),
        pl.col('inspect_type').is_in(['OPI', 'OWN', 'REI']).not_()
    )
)

license_tracker_file_id = os.environ['PI_LICENSE_TRACKER_FILE']
licenses = (
    drive.lazyframe_from_id_and_sheetname(file_id=license_tracker_file_id, sheet_name='Form Responses 1', infer_schema_length=0)  # read_excel() does not have infer_schema
    .select(
        pl.col('Timestamp').str.to_date('%Y-%m-%d %H:%M:%S%.f').alias('submit_date'),
        pl.col('Permit Number').str.strip_chars().str.to_uppercase().alias('permit_number'),
    )
    .filter(pl.col('submit_date') >= last_last_mo)
)

inspections_wo_licenses_submission = (
    inspections
    .join(licenses, on='permit_number', how='anti')
    .sort('inspection_date')
)

iwls_df = inspections_wo_licenses_submission.collect()
print('inspections without licenses sumbission:')
print(iwls_df)

fn = Path('data/inspections_wo_licenses_submission.csv')
iwls_df.write_csv(fn)
print(f'{fn} written')
