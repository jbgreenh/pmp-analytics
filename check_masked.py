import argparse
import os
from datetime import datetime, timedelta

import polars as pl
from az_pmp_utils import auth, drive
from df_compare_pl import df_compare
from dotenv import load_dotenv
from googleapiclient.discovery import build

from constants import PHX_TZ

parser = argparse.ArgumentParser(description='check masked extract')
parser.add_argument('-o', '--old', type=int, default=0, help='check as if n months in the past')
args = parser.parse_args()

load_dotenv()
creds = auth.auth()
service = build('drive', 'v3', credentials=creds)

current_month_date = datetime.now(tz=PHX_TZ).replace(day=1)
for _ in range(args.old):
    current_month_date = current_month_date.replace(day=1) - timedelta(days=1)
mask_month = current_month_date.month
mask_year = current_month_date.year - 7
prev_file_m_d = current_month_date.replace(day=1, year=mask_year) - timedelta(days=1)
prev_month = prev_file_m_d.month
prev_year = prev_file_m_d.year

folder = os.environ['MASKED_EXTRACT_FOLDER']
print('getting folder ids...')
year_folder = drive.folder_id_from_name(folder_name=f'{mask_year}', parent_folder_id=folder, service=service)
prev_year_folder = drive.folder_id_from_name(folder_name=f'{prev_year}', parent_folder_id=folder, service=service)

mask_fn = f'AZ_{mask_year}{str(mask_month).zfill(2)}_masked.csv'
mask_file = drive.lazyframe_from_file_name(file_name=mask_fn, folder_id=year_folder, drive_ft='csv', service=service, separator='|', infer_schema=False).collect()
prev_fn = f'AZ_{prev_year}{str(prev_month).zfill(2)}_masked.csv'
prev_file = drive.lazyframe_from_file_name(file_name=prev_fn, folder_id=prev_year_folder, drive_ft='csv', service=service, separator='|', infer_schema=False).collect()

print('-----')
print(f'comparing a:{mask_fn} and b:{prev_fn}...')
print('-----')
if df_compare(mask_file, prev_file, col_only=True):
    print('columns are equal')
print('-----')
print(f'{mask_fn} row count: {mask_file.height}')
print(f'{prev_fn} row count: {prev_file.height}')
percent_change = round((((mask_file.height - prev_file.height) / prev_file.height) * 100), 2)
print(f'percent change: {percent_change}')
print('-----')
filled_dates = (
    mask_file
    .with_columns(
        pl.col('dispensation_filled_at').str.to_date('%Y-%m-%d')
    )
)
min_filled_date = filled_dates.select(pl.col('dispensation_filled_at').min()).item().strftime('%Y-%m-%d')
max_filled_date = filled_dates.select(pl.col('dispensation_filled_at').max()).item().strftime('%Y-%m-%d')
print(f'{mask_fn} {min_filled_date = }')
print(f'{mask_fn} {max_filled_date = }')
print('-----')
sample = mask_file.sample(20)
sample.write_clipboard()
sample.write_csv('data/mask_sample.csv')
print('data/mask_sample.csv updated and written to clipboard')
