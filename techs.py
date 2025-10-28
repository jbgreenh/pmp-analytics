import os
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import polars as pl
from dotenv import load_dotenv
from googleapiclient.discovery import build

from utils import auth, email, files
from utils.constants import PHX_TZ

last_mo = datetime.now(tz=PHX_TZ).date().replace(day=1) - timedelta(days=1)

t_fp = Path('data/techs.xls')
files.warn_file_age(t_fp)
techs = (
    pl.from_pandas(pd.read_html(t_fp, header=1)[0])
    .with_columns(
        pl.col(['Expiration Date', 'Application Date', 'Issue Date']).str.to_date('%m/%d/%Y')
    )
    .filter(
        (pl.col('Status').str.to_lowercase().str.starts_with('open')) &
        ((pl.col('Issue Date').dt.year() == last_mo.year) & (pl.col('Issue Date').dt.month() == last_mo.month))
    )
)

s_fp = Path('data/superseded.xls')
files.warn_file_age(s_fp)
superseded = (
    pl.from_pandas(pd.read_html(s_fp, header=1)[0])
    .with_columns(
        pl.col(['Expiration Date', 'Application Date', 'Issue Date']).str.to_date('%m/%d/%Y')
    )
)

s_to_t = (
    techs.join(superseded, on='SSN', how='inner', suffix='_sup')
    .with_columns(
        pl.col('Issue Date').sub(pl.col('Issue Date_sup')).alias('time_delta'),
        pl.col('Issue Date').sub(pl.col('Expiration Date_sup')).alias('time_delta2'),
    )
    .with_columns(
        pl.col('time_delta').dt.total_days().alias('days_to_tech'),
        pl.col('time_delta2').dt.total_days().alias('days_to_tech_from_exp'),
    )
    .select(
        'License #', 'Type', 'Type_sup', 'Status', 'Status_sup',
        'First Name', 'Middle Name', 'Last Name', 'Issue Date',
        'Issue Date_sup', 'Expiration Date_sup', 'days_to_tech', 'days_to_tech_from_exp'
    )
)

pl.Config.set_tbl_hide_dataframe_shape(True)
pl.Config.set_tbl_hide_column_data_types(True)
dtt = s_to_t.select('days_to_tech').describe()
dttfe = s_to_t.select('days_to_tech_from_exp').describe()

load_dotenv()
sup_sheet = os.environ['SUPERSEDED_FILE']
to = os.environ['EMAIL_SUP']
sender = os.environ['EMAIL_DATA']
signature = os.environ['EMAIL_DATA_SIG'].replace(r'\n', '\n')

s_to_t = (
    s_to_t
    .with_columns(
        pl.col(['Issue Date', 'Issue Date_sup', 'Expiration Date_sup']).dt.to_string('%Y-%m-%d')
    )
)
data = [list(row) for row in s_to_t.rows()]
data.insert(0, s_to_t.columns)

sheet_name = f'{last_mo.year}{str(last_mo.month).zfill(2)}'

creds = auth.auth()
service = build('sheets', 'v4', credentials=creds)

print('adding sheet...')
service.spreadsheets().batchUpdate(
    spreadsheetId=sup_sheet,
    body={
        "requests": [
            {"addSheet": {"properties": {"title": sheet_name}}}
        ]
    },
).execute()

print('updating new sheet...')
service.spreadsheets().values().update(
    spreadsheetId=sup_sheet,
    range=f"{sheet_name}!A1",
    valueInputOption="RAW",
    body={"values": data},
).execute()

sheet_link = f'https://docs.google.com/spreadsheets/d/{sup_sheet}'
email_body = f'hi all,\n\nthe superseded to tech sheet has been updated to include {last_mo.month}/{last_mo.year} data: {sheet_link}.\n\nbelow you can find descriptive statistics for {last_mo.month}/{last_mo.year}:\n\ndays to tech:\n{dtt}\ndays to tech from exp:\n{dttfe}{signature}'
subject = f'{last_mo.month}/{last_mo.year} superseded to tech update'
message = email.EmailMessage(sender=sender, to=to, subject=subject, message_text=email_body, monospace=True)

email.send_email(message)
