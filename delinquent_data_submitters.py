# daily notices (along with list of closed pharmacies in manage pharmacies)
# special notices on fridays
# hard deadline 30 business days after first friday notice
# email logs with sent_dt, to, permit_number, days_delinquent, zip, email_type
# other sheet will have active 7+ days delinquent pharmacies
# last sheet will have pharmacies who have had complaints opened
import os
from calendar import FRIDAY, WEDNESDAY
from datetime import datetime, timedelta
from pathlib import Path
from time import sleep
from typing import Literal

import polars as pl
from dotenv import load_dotenv

from utils import drive, files, num_and_dt
from utils.constants import DAYS_DELINQUENT_THRESHOLD, PHX_TZ

type EmailType = Literal['daily', 'friday']


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


def process_input_files(mp_path: Path, dds_path: Path, lr_path: Path) -> pl.LazyFrame:
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
        # TODO: exclude pharmacies that have active complaints in DDS_COMPLAINTS_FILE
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


def date_in_next_week(lf: pl.LazyFrame) -> pl.LazyFrame:
    today = datetime.now(tz=PHX_TZ).date()
    days_to_mon = 7 - today.weekday()
    next_mon = today + timedelta(days=days_to_mon)
    next_fri = next_mon + timedelta(days=FRIDAY)
    return (
        lf
        .filter(
            pl.col('deadline').str.to_date('%Y-%m-%d').is_between(next_mon, next_fri)
        )
    )


def send_notices(dds: pl.LazyFrame, email_type: EmailType) -> None:
    if email_type == 'friday':
        print('friday notices')
        # TODO: set email subj, body, from, to, etc
        notices = dds.collect()
    else:
        print('daily notices')
        # TODO: set email subj, body, etc
        notices = dds.collect()

    timestamps = []
    for _row in notices.iter_rows(named=True):
        # TODO: send emails bcc: compliance
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


def pharm_clean(dds: pl.LazyFrame) -> None:
    today = datetime.now(tz=PHX_TZ).date()
    if WEDNESDAY <= today.weekday() <= FRIDAY:  # remove pharmacies that are no longer delinquent from deadlines list
        deadlines = drive.lazyframe_from_id_and_sheetname(os.environ['DDS_DEADLINES_FILE'], 'deadlines', infer_schema_length=0)  # read_excel does not have infer_schema
        updated_deadlines = (
            deadlines
            .join(dds, on='Pharmacy License Number', how='semi')
        )
        deadlines_path = Path('deadlines.csv')
        updated_deadlines.collect().write_csv(deadlines_path)
        drive.update_sheet(deadlines_path, os.environ['DDS_DEADLINES_FILE'])
        deadlines_path.unlink()

        if today.weekday() == WEDNESDAY:  # notify compliance team of deadlines that fall in the next week
            due_next_week = date_in_next_week(updated_deadlines)
            # TODO: email compliance team with due_next_week

    if today.weekday() == FRIDAY:  # add new deadlines to list, send friday notices
        # TODO: add schema to deadlines so join doesn't complain if deadlines file is empty. see: lf.match_to_schema
        deadlines = drive.lazyframe_from_id_and_sheetname(os.environ['DDS_DEADLINES_FILE'], 'deadlines', infer_schema_length=0)  # read_excel does not have infer_schema
        today = datetime.now(tz=PHX_TZ).date()
        due_date = num_and_dt.add_business_days(today)
        new_deadlines = (
            dds
            .filter(
                pl.col('Days Delinquent').cast(pl.Int64) >= DAYS_DELINQUENT_THRESHOLD |
                pl.col('Days Delinquent') == ''  # noqa: PLC1901 | empty string is not falsey in polars
            )
            # TODO: select cols to match DDS_DEADLINES_FILE
            .join(deadlines, on='Pharmacy License Number', how='anti')
        )
        if new_deadlines.collect().height > 0:
            new_deadlines = (
                new_deadlines
                .with_columns(
                    pl.lit(due_date).dt.to_string('%Y-%m-%d').alias('deadline')
                )
            )
            full_deadlines = pl.concat([deadlines, new_deadlines])
            deadlines_path = Path('deadlines.csv')
            full_deadlines.collect().write_csv(deadlines_path)
            drive.update_sheet(deadlines_path, os.environ['DDS_DEADLINES_FILE'])
            deadlines_path.unlink()

        send_notices(dds, 'friday')
    else:
        send_notices(dds, 'daily')


if __name__ == '__main__':
    load_dotenv()
    mp_path = Path('data/pharmacies.csv')
    files.warn_file_age(mp_path)

    dds_path = Path('data/DelinquentDispenserRequest.csv')
    files.warn_file_age(dds_path)

    lr_path = Path('data/List Request.csv')
    files.warn_file_age(lr_path)

    dds = process_input_files(mp_path, dds_path, lr_path)
    pharm_clean(dds)
