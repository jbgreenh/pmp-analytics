import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from df_compare_pl import df_compare
from dotenv import load_dotenv
from googleapiclient.discovery import build

from utils import auth, drive

load_dotenv()
creds = auth.auth()
service = build('drive', 'v3', credentials=creds)

last_month_date = datetime.now(tz=ZoneInfo('America/Phoenix')).replace(day=1) - timedelta(days=1)
mask_month = last_month_date.month
mask_year = last_month_date.year - 7
prev_file_m_d = last_month_date.replace(day=1, year=mask_year) - timedelta(days=1)
prev_month = prev_file_m_d.month
prev_year = prev_file_m_d.year

folder = os.environ['MASKED_EXTRACT_FOLDER']
print('getting folder ids...')
year_folder = drive.folder_id_from_name(service, folder_name=f'{mask_year}', parent_id=folder)
prev_year_folder = drive.folder_id_from_name(service, folder_name=f'{prev_year}', parent_id=folder)

mask_fn = f'AZ_{mask_year}{str(mask_month).zfill(2)}_masked.csv'
mask_file = drive.lazyframe_from_file_name(service, file_name=mask_fn, folder_id=year_folder, drive_ft='csv', separator='|', infer_schema_length=None).collect()
prev_fn = f'AZ_{prev_year}{str(prev_month).zfill(2)}_masked.csv'
prev_file = drive.lazyframe_from_file_name(service, file_name=prev_fn, folder_id=prev_year_folder, drive_ft='csv', separator='|', infer_schema_length=None).collect()

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
mask_file.sample(20).write_clipboard()
mask_file.sample(20).write_csv('data/mask_sample.csv')
print('data/mask_sample.csv updated and written to clipboard')
