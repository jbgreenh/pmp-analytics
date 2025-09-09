import os
import pathlib
from datetime import date

import polars as pl
from dotenv import load_dotenv
from googleapiclient.discovery import build

from utils import auth, drive, email

# TODO: use new email util


def pharm_clean():
    """shape data for final report"""
    today = date.today().strftime("%m-%d-%Y")

    mp = (
        pl.scan_csv('data/pharmacies.csv')
        .with_columns(
            pl.col('DEA').str.strip_chars().str.to_uppercase()
        )
        .select('DEA', 'Pharmacy License Number')
    )

    igov = (
        pl.scan_csv('data/List Request.csv', infer_schema_length=0)
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

    ddr = (
        pl.scan_csv('data/DelinquentDispenserRequest.csv')
        .filter((pl.col('Days Delinquent') >= 7) | (pl.col('Days Delinquent').is_null()))
        .with_columns(
            pl.col('DEA').str.strip_chars().str.to_uppercase()
        )
        .join(mp, on='DEA', how='left', coalesce=True)
        .with_columns(
            pl.col('Pharmacy License Number').str.strip_chars().str.to_uppercase()
        )
        .join(igov, on='Pharmacy License Number', how='left', coalesce=True)
        .sort(by=['Status', 'Pharmacy License Number', 'Days Delinquent'], descending=True)
        .with_columns(
            pl.concat_str(
                pl.lit(', '),
                pl.col('Apt/Suite #')
            ).alias('Apt/Suite #').fill_null(''),
            pl.concat_str(
                pl.col('Street Address'),
                pl.col('Apt/Suite #')
            ).alias('Street Address').fill_null(pl.col('Pharmacy Address')),
            pl.col('Business Name').fill_null(pl.col('Pharmacy Name')),
            pl.col('Last Compliant').str.to_date('%Y-%m-%d').dt.strftime('%m/%d/%Y'),
            pl.lit(today).alias('Date List Pulled')
        )
        .rename(
            {
                'Primary Email': 'awarxe_email', 'Email': 'igov_email',
                'Primary Phone': 'awarxe_phone', 'Phone': 'igov_phone'
            }
        )
        .select(
            'Business Name', 'Street Address', 'City', 'State', 'Zip', 'Pharmacy License Number', 'DEA',
            'Status', 'Days Delinquent', 'Last Compliant', 'Date List Pulled', 'awarxe_email', 'igov_email',
            'awarxe_phone', 'igov_phone'
        )
    )

    closed = (
        ddr
        .filter(pl.col('Status').str.contains('CLOSE'))
        .select('Business Name', 'Pharmacy License Number', 'DEA', 'Status', 'Days Delinquent')
        .collect()
    )

    if not closed.is_empty():
        print('closed pharmacies, update in manage pharmacies in awarxe:')
        print(closed)
    else:
        print('no closed pharmacies')

    fname = f'{today}.csv'
    ddr.collect().write_csv(fname)
    return fname


def main():
    creds = auth.auth()
    load_dotenv()
    fname = pharm_clean()

    service = build('drive', 'v3', credentials=creds)
    folder_id = os.environ['PHARM_CLEAN_FOLDER']

    drive.upload_csv_as_sheet(service=service, file_name=fname, folder_id=folder_id)

    pathlib.Path(fname).unlink()

    sender = os.environ['EMAIL_DATA']
    to = os.environ['EMAIL_COMPLIANCE']
    subject = 'delinquent submitters cleanup complete'
    # leaving links out as requested
    message_txt = 'hello, the weekly delinquent data submitters cleanup is complete\n\nthank you,\n\ndata team'

    message = email.EmailMessage(sender=sender, to=to, subject=subject, message_text=message_txt)
    email.send_email(message)


if __name__ == '__main__':
    main()
