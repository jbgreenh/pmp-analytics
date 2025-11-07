# daily notices (along with list of closed pharmacies in manage pharmacies)
# special notices on fridays
# hard deadline 30 business days after first friday notice
# email logs with sent_dt, to, permit_number, days_delinquent, zip, email_type
# other sheet will have active 7+ days delinquent pharmacies
# last sheet will have pharmacies who have had complaints opened
from pathlib import Path

import polars as pl

from utils import files

mp_path = Path('data/pharmacies.csv')
files.warn_file_age(mp_path)
mp = (
    pl.scan_csv(mp_path)
    .with_columns(
        pl.col('DEA').str.strip_chars().str.to_uppercase()
    )
    .filter(
        pl.col('Reporting Requirements') != 'Exempt'
    )
    .select('DEA', 'Pharmacy License Number', 'Reporting Requirements')
)

lr_path = Path('data/List Request.csv')
files.warn_file_age(lr_path)
igov = (
    pl.scan_csv(lr_path, infer_schema=False)
    .filter(
        pl.col('Type') == 'Pharmacy'
    )
    .with_columns(
        pl.col('License/Permit #').str.strip_chars().str.to_uppercase()
    )
    .rename(
        {'License/Permit #': 'Pharmacy License Number'}
    )
    .select(
        'Pharmacy License Number', 'Status', 'Business Name', 'Street Address', 'Apt/Suite #',
        'City', 'State', 'Zip', 'Email', 'Phone'
    )
)

mp_closed = (
    mp
    .join(igov, on='Pharmacy License Number', how='left', coalesce=True)
    .filter(
        pl.col('Status').str.starts_with('OPEN').not_()
    )
)
mp_closed.collect().write_clipboard()
