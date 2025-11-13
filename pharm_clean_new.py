# daily notices (along with list of closed pharmacies in manage pharmacies)
# special notices on fridays
# hard deadline 30 business days after first friday notice
# email logs with sent_dt, to, permit_number, days_delinquent, zip, email_type
# other sheet will have active 7+ days delinquent pharmacies
# last sheet will have pharmacies who have had complaints opened
import os
from calendar import FRIDAY, SATURDAY, TUESDAY
from datetime import datetime
from pathlib import Path
from time import sleep

import polars as pl
from dotenv import load_dotenv

from utils import drive, num_and_dt
from utils.constants import PHX_TZ


def find_closed(mp_path: Path, lr_path: Path) -> pl.LazyFrame:
    """
    finds pharmacies from manage pharmacies that are not open in igov

    args:
        mp_path: path to the manage pharmacies file
        lr_path: path to the list request file

    returns:
        list of pharmacies from manage pharmacies that are not open in igov as a lazyframe
    """
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

    return (
        mp
        .join(igov, on='Pharmacy License Number', how='left', coalesce=True)
        .filter(
            pl.col('Status').str.starts_with('OPEN').not_()
        )
    )


def process_files(mp_path: Path, dds_path: Path, lr_path: Path) -> pl.LazyFrame:
    """
    process files, send email notifications, and update logs

    args:
        mp_path: path to manage pharmacies file
        dds_path: path to delinquent data submitters file
        lr_path: path to list request file

    returns:
        processed dds file as a lazyframe
    """
    mp = (
        pl.scan_csv(mp_path)
        .filter(
            pl.col('Reporting Requirements') != 'Exempt'
        )
        .select(
            pl.col('DEA').str.strip_chars().str.to_uppercase(),
            'Pharmacy License Number',
            pl.col('Pharmacist Email').str.to_lowercase().alias('mp_email'),
            pl.col('Phone Number').alias('mp_phone')
        )
    )

    lr = (
        pl.scan_csv(lr_path, infer_schema=False)
        .filter(
            pl.col('Type') == 'Pharmacy'
        )
        .select(
            pl.col('License/Permit #').str.strip_chars().str.to_uppercase().alias('Pharmacy License Number'),
            'Status',
            'Business Name',
            'Street Address',
            'Apt/Suite #',
            'City',
            'State',
            'Zip',
            pl.col('Email').str.to_lowercase().alias('igov_email'),
            pl.col('Phone').alias('igov_phone')
        )
    )

    return (
        pl.scan_csv(dds_path)
        .with_columns(
            pl.col('Last Compliant').fill_null('never submitted')
        )
        .join(mp, on='DEA', how='left', coalesce=True)
        .join(lr, on='Pharmacy License Number', how='left', coalesce=True)
        .filter(
            pl.col('Status').str.starts_with('OPEN')
        )
        .select(
            'DEA',
            'Pharmacy License Number',
            'Business Name',
            'Status',
            'Last Compliant',
            'Days Delinquent',
            'Primary User',
            'Zip',
            pl.concat_list(pl.col('Primary Email').str.to_lowercase(), pl.col('mp_email'), pl.col('igov_email')).list.unique().list.join(',').alias('to')
        )
        .sort(pl.col('Days Delinquent'), descending=True)
    )


def send_notices(dds: pl.LazyFrame) -> None:
    today = datetime.now(tz=PHX_TZ).date()
    if today.weekday() > TUESDAY and today.weekday() < SATURDAY:
        # TODO: read in active 7+, check if PAST(> not >=) due date from that file, move those to complaint
        # must be thursday or friday for this check (maybe wednesday is ok too?)
        # instead of moving to complaint, find last wednesday before due date and notify compliance email about it
        pass
    if today.weekday() == FRIDAY:
        due_date = num_and_dt.add_business_days(today)
        print(f'friday notices, {due_date = }')
        email_type = 'friday'
        # TODO: antijoin and concat with active 7+, filter, set email subj, body, etc
        notices = dds.collect()
    else:
        print('daily notices')
        email_type = 'daily'
        # TODO: set email subj, body, etc
        notices = dds.collect()

    timestamps = []
    for _row in notices.iter_rows(named=True):
        # TODO: send emails (triple quote f string) bcc: compliance
        sleep(.25)
        ts = datetime.now(tz=PHX_TZ)
        timestamps.append(ts)
    ts_series = pl.Series(name='sent_dt', values=timestamps, dtype=pl.Datetime)
    notices.insert_column(0, ts_series)

    logs = drive.lazyframe_from_id_and_sheetname(os.environ['DDS_EMAIL_LOGS_FILE'], 'dds_email_logs', infer_schema_length=0).collect()  # read_excel does not have infer_schema
    new_dds_log = (
        notices
        .select(
            pl.col('sent_dt').dt.to_string('iso'),
            'to',
            pl.col('Pharmacy License Number').alias('permit_number'),
            pl.col('DEA').alias('dea'),
            pl.col('Days Delinquent').cast(pl.String).alias('days_delinquent'),
            pl.concat_str(pl.lit("'"), pl.col('Zip').cast(pl.String)).alias('zip'),  # to preserve leading zeros
            pl.lit(email_type).alias('email_type')
        )
    )
    full_logs = pl.concat([logs, new_dds_log])
    fl_path = Path('full_logs.csv')
    full_logs.write_csv(fl_path)
    drive.update_sheet(fl_path, os.environ['DDS_EMAIL_LOGS_FILE'])
    fl_path.unlink()


if __name__ == '__main__':
    load_dotenv()
    mp_path = Path('data/pharmacies.csv')
    # files.warn_file_age(mp_path)

    dds_path = Path('data/DelinquentDispenserRequest.csv')
    # files.warn_file_age(dds_path)

    lr_path = Path('data/List Request.csv')
    # files.warn_file_age(lr_path)

    dds = process_files(mp_path, dds_path, lr_path)
    send_notices(dds)
