import argparse
import os
import sys
from calendar import FRIDAY, WEDNESDAY
from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal

import polars as pl
from az_pmp_utils import auth, drive, email, files, num_and_dt
from dotenv import load_dotenv
from googleapiclient import errors
from googleapiclient.discovery import build

from constants import (
    DAILY_DAYS_DELINQUENT_THRESHOLD,
    PHX_TZ,
    WEEKLY_DAYS_DELINQUENT_THRESHOLD,
)

type EmailType = Literal['daily', 'friday']


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
        pl.scan_csv(mp_path, infer_schema=False)
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
            pl.col('Type') == 'Pharmacy',
            pl.col('Status').str.starts_with('OPEN')
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
        .unique()  # this file has an entry for each PIC
    )

    complaints = (
        drive.lazyframe_from_id_and_sheetname(os.environ['DDS_COMPLAINTS_FILE'], 'complaints', infer_schema_length=0)  # read_excel does not have infer_schema
        .select(
            'Pharmacy License Number',
            'complaint_status'
        )
        .filter(
            pl.col('complaint_status') == 'Open'
        )
    )

    return (
        pl.scan_csv(dds_path, infer_schema=False)
        .filter(
            (pl.col('Days Delinquent').str.to_integer() >= DAILY_DAYS_DELINQUENT_THRESHOLD) |  # account for bamboo's mishandling of time zones
            (pl.col('Days Delinquent') == '') |                                                # noqa: PLC1901 | empty string is not falsey in polars
            (pl.col('Days Delinquent').is_null())
        )
        .join(mp, on='DEA', how='left')
        .join(lr, on='Pharmacy License Number', how='inner')
        .join(complaints, on='Pharmacy License Number', how='anti')
        .select(
            'DEA',
            'Pharmacy License Number',
            'Business Name',
            'Status',
            (pl.col('Last Compliant').str.to_date('%Y-%m-%d') + pl.duration(days=1)).dt.to_string('%Y-%m-%d').alias('Last Compliant'),
            'Days Delinquent',
            'Primary User',
            pl.concat_list(pl.col('Primary Email').str.to_lowercase(), pl.col('mp_email'), pl.col('igov_email')).list.unique().list.join(',').alias('to'),
            pl.concat_list(pl.col('Primary Phone').str.to_lowercase(), pl.col('mp_phone'), pl.col('igov_phone')).list.unique().list.join(',').alias('phone_numbers'),
            'Street Address',
            'Apt/Suite #',
            'City',
            'State',
            'Zip'
        )
    )


def date_in_next_week(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    filters a given lazyframe for rows with deadlines in the next business week (mon-fri)

    args:
        lf: a lazyframe with a `deadline` column in `'YYYY-MM-DD` format

    returns:
        the filtered lazyframe
    """
    today = datetime.now(tz=PHX_TZ).date()
    days_to_mon = 7 - today.weekday()
    next_mon = today + timedelta(days=days_to_mon)
    next_fri = next_mon + timedelta(days=FRIDAY)
    return (
        lf
        .filter(
            pl.col('deadline').str.to_date("%Y-%m-%d").is_between(next_mon, next_fri)
        )
    )


def send_notices(lf: pl.LazyFrame, email_type: EmailType) -> None:
    """
    send dds email notices (or create drafts)

    args:
        lf: a lazyframe with the dds recipients
        email_type: daily or friday notices
    """
    service = build('gmail', 'v1', credentials=auth.auth())
    timestamps = []
    notices = lf.sort(pl.col('Last Compliant').str.to_date("%Y-%m-%d")).collect()

    if args.send_emails:
        sanity_check = input(f'{notices.height} notices to be sent, does this make sense? (y/n): ')
        if sanity_check != 'y':
            sys.exit('no notices sent, verify data with vendor')

    for row in notices.iter_rows(named=True):
        pharmacy_address = f'{row['Street Address']}, {row['Apt/Suite #']}\n{row['City']}, {row['State']} {row['Zip'][1:]}' if row['Apt/Suite #'] else f'{row['Street Address']}\n{row['City']}, {row['State']} {row['Zip'][1:]}'
        if row['Last Compliant'] is not None:
            if row['Last Compliant'] == (datetime.now(tz=PHX_TZ).date() - timedelta(days=2)).strftime('%Y-%m-%d'):
                last_compliant = row['Last Compliant']
            else:
                last_compliant = f'{row['Last Compliant']} - {(datetime.now(tz=PHX_TZ).date() - timedelta(days=2)).strftime('%Y-%m-%d')}'
        else:
            last_compliant = 'no data has ever been received'

        if email_type == 'friday':
            subject = f'CSPMP Action Required: Possible Complaint Against {row['Pharmacy License Number']}'
            body = f"""ATTENTION: {row['Business Name']}
{pharmacy_address}
{row['Pharmacy License Number']}
{row['DEA']}

According to our records, your pharmacy is not submitting daily reports to the Arizona Controlled Substance Prescription Monitoring Program clearinghouse.

Dispensation data is missing for: <b>{last_compliant}</b>

At this time, you are in violation of <a href="https://www.azleg.gov/ars/36/02608.htm" target="_blank">ARS § 36-2608</a> reporting requirements. <em><b>Failure to upload your delinquent schedule II-V dispensations will result in a complaint being opened against the pharmacy.</b></em>

<b>Zero reports should be submitted for any days there are no controlled substance dispensations. For days you are not operational, you should report zero for those days on your next open business day.</b>

<b style='color: red;'>Please ensure you upload your missed submissions by {row['deadline'][1:]} or a complaint will be opened against the pharmacy permit. </b>

If your pharmacy has an active DEA number, an active AZ pharmacy permit, and is not limited to veterinary dispensing, <em><b>it is required to submit a daily report, including zero reports, for controlled substances II-V.</b></em>

If your pharmacy utilizes a vendor to submit dispensations on your behalf, please contact your vendor immediately to get this issue corrected to avoid possible Board action. You may forward this email to the appropriate vendor contact for assistance.

If you are receiving this message and you are a data vendor reporting submissions on behalf of the pharmacy, please be aware that the pharmacy is delinquent in AZ CSPMP reporting and faces possible Board action if not corrected by the given deadline. Please forward this information to the appropriate members of the pharmacy team if necessary.

If you have any questions or concerns about the data submission process, please contact Bamboo Health for Technical Support directly at 1-855-929-4767. Technical Support is available 24 hours a day, 7 days a week.

<a href="https://drive.google.com/file/d/1R1wCymw9T5n2sqn8fQGuWeEoCChXmjB0/view?ts=67ca02cd" target="_blank">AZ Data Submission Dispenser Guide</a>
<a href="https://pharmacypmp.az.gov/data-submissions-faqs" target="_blank">AZ Data Submission FAQs</a>{os.environ['EMAIL_COMP_SIG'].replace(r'\n', '\n')}
            """
        else:
            subject = f'Notice of Missing CSPMP Data Submissions for {row['Pharmacy License Number']}'
            body = f"""<b>At this time, your pharmacy, {row['Pharmacy License Number']}, is in violation of <a href="https://www.azleg.gov/ars/36/02608.htm" target="_blank">ARS § 36-2608</a> reporting requirements.</b>

You are receiving this email because you are listed as the party responsible for submitting controlled substance dispensing information for the above-referenced dispenser to the Arizona Controlled Substances Prescription Monitoring Program (AZ CSPMP). Controlled substance dispensing information is missing for: <b>{last_compliant}</b>.

Please upload your schedule II-V dispensations DAILY, including zero reports, to avoid being noncompliant, and make sure to upload any days that were missed.

*Businesses closed for the weekend will still need to report on the following business day (Ex. on Monday report zero reports for Saturday and Sunday).

If you have any questions or concerns about the data submission process, please contact Bamboo Health for Technical Support directly at 1-855-929-4767. Technical Support is available 24 hours a day, 7 days a week.

<a href="https://drive.google.com/file/d/1R1wCymw9T5n2sqn8fQGuWeEoCChXmjB0/view?ts=67ca02cd" target="_blank">AZ Data Submission Dispenser Guide</a>
<a href="https://pharmacypmp.az.gov/data-submissions-faqs" target="_blank">AZ Data Submission FAQs</a>{os.environ['EMAIL_COMP_SIG'].replace(r'\n', '\n')}
            """
        msg = email.EmailMessage(
            sender=os.environ['EMAIL_COMPLIANCE'],
            to=row['to'],
            bcc=os.environ['EMAIL_COMPLIANCE'],
            subject=subject,
            message_text=body,
            monospace=True,
        )
        try:
            email.send_email(msg, service=service, draft=(not args.send_emails))
            ts = datetime.now(tz=PHX_TZ)
        except errors.HttpError as error:
            print(f'failed to send message for {row['Pharmacy License Number']} | {row['DEA']}:')
            print(f'error: {error!s}')
            print('`sent_dt` will be left blank')
            ts = None
        timestamps.append(ts)

    print(f'{len(timestamps)} {'emails sent' if args.send_emails else 'drafts created'}')
    ts_series = pl.Series(name='sent_dt', values=timestamps, dtype=pl.Datetime)
    notices.insert_column(0, ts_series)

    logs = (
        drive.lazyframe_from_id_and_sheetname(os.environ['DDS_EMAIL_LOGS_FILE'], 'dds_email_logs', infer_schema_length=0)  # read_excel does not have infer_schema
        .collect()
    )
    new_dds_log = (
        notices
        .select(
            pl.col('sent_dt').dt.to_string('iso'),
            'to',
            pl.col('Pharmacy License Number').alias('permit_number'),
            pl.col('DEA').alias('dea'),
            pl.col('Last Compliant').alias('last_compliant'),
            pl.col('Zip').alias('zip'),
            pl.lit(email_type).alias('email_type')
        )
    )
    full_logs = pl.concat([logs, new_dds_log])
    fl_path = Path('full_logs.csv')
    full_logs.write_csv(fl_path)
    drive.update_sheet(fl_path, os.environ['DDS_EMAIL_LOGS_FILE'], sheet_name='dds_email_logs')
    fl_path.unlink()


def missed_deadlines_to_complaint(deadlines: pl.LazyFrame) -> pl.DataFrame | None:
    """
    moves pharmacies who have missed their deadline to the complaints sheet and generates required documents

    args:
        deadlines: a lazyframe of the deadlines sheet

    returns:
        a dataframe with the pharmacies that have been added to the complaint sheet,
        or None if no pharmacies were added
    """
    today = datetime.now(tz=PHX_TZ).date()
    missed_dl = (
        deadlines
        .filter(
            pl.col('deadline').str.to_date('%Y-%m-%d') < today
        )
        .collect()
    )

    not_missed = (
        deadlines
        .filter(
            pl.col('deadline').str.to_date('%Y-%m-%d') >= today
        )
        .collect()
    )

    if missed_dl.height > 0:
        print('missed deadlines:')
        print(missed_dl)
        print('moving to dds_complaints...')

        folder_ids = []
        dds_compaints_sheet_id = os.environ['DDS_COMPLAINTS_FILE']
        for row in missed_dl.iter_rows(named=True):
            complaint_folder_id = drive.folder_id_from_name(folder_name=row['Business Name'] + '-' + row['Pharmacy License Number'], parent_folder_id=os.environ['PHARMACY_REPORTING_COMPLAINTS_FOLDER'], create=True)
            folder_ids.append(complaint_folder_id)
            complaint_folder_link = f'https://drive.google.com/drive/folders/{complaint_folder_id}'
            print(f'{complaint_folder_link = }')

            service = build('sheets', 'v4', credentials=auth.auth())
            result = service.spreadsheets().values().get(spreadsheetId=dds_compaints_sheet_id, range='complaints!A:A').execute()
            values = result.get('values', [])

            last_row = len(values) if values else 1

            data = [complaint_folder_link, '', '', '', '', '']
            data.extend(list(row.values()))
            data_range = f'complaints!A{last_row + 1}'

            service.spreadsheets().values().update(
                spreadsheetId=dds_compaints_sheet_id,
                range=data_range,
                valueInputOption='RAW',
                body={'values': [data]}
            ).execute()

            print(f'updated dds_complaints: https://docs.google.com/spreadsheets/d/{dds_compaints_sheet_id}')

        fl_path = Path('temp_csv.csv')
        not_missed.write_csv(fl_path)
        drive.update_sheet(fl_path, os.environ['DDS_DEADLINES_FILE'], sheet_name='dds_deadlines')
        fl_path.unlink()
        return missed_dl.with_columns(pl.Series('folder_id', folder_ids))

    print('no missed deadlines')
    return None


def generate_complaint_docs(new_complaints: pl.DataFrame) -> None:
    """
    adds the complaint docs to the folder created in `missed_deadlines_to_complaint`

    args:
        new_complaints: the df returned by `missed_deadlines_to_complaint`
    """
    print('generating complaint docs...')
    creds = auth.auth()
    docs_service = build('docs', 'v1', credentials=creds)
    drive_service = build('drive', 'v3', credentials=creds)

    for row in new_complaints.iter_rows(named=True):
        res = 'resident' if row['State'] == 'AZ' else 'non-resident'
        address = f'{row['Street Address']}\n{row['Apt/Suite #']}' if row['Apt/Suite #'] else row['Street Address']

        complaint_summary_id = drive_service.files().copy(
            fileId=os.environ['DDS_COMPLAINT_SUMMARY_FILE'],
            body={
                'name': f'{row['Business Name']} Complaint Summary',
                'parents': [row['folder_id']]
            }, supportsAllDrives=True
         ).execute()['id']

        notice_of_complaint_id = drive_service.files().copy(
            fileId=os.environ['DDS_NOTICE_OF_COMPLAINT_FILE'],
            body={
                'name': f'{row['Business Name']} Notice of Complaint',
                'parents': [row['folder_id']]
            }, supportsAllDrives=True
         ).execute()['id']

        requests = [
            {
                'replaceAllText': {
                    'containsText': {'text': '{{bus_name}}', 'matchCase': True},
                    'replaceText': f'{row['Business Name']}'
                }
            },
            {
                'replaceAllText': {
                    'containsText': {'text': '{{address}}', 'matchCase': True},
                    'replaceText': f'{address}'
                }
            },
            {
                'replaceAllText': {
                    'containsText': {'text': '{{city}}', 'matchCase': True},
                    'replaceText': f'{row['City']}'
                }
            },
            {
                'replaceAllText': {
                    'containsText': {'text': '{{state}}', 'matchCase': True},
                    'replaceText': f'{row['State']}'
                }
            },
            {
                'replaceAllText': {
                    'containsText': {'text': '{{zip}}', 'matchCase': True},
                    'replaceText': f'{row['Zip']}'
                }
            },
            {
                'replaceAllText': {
                    'containsText': {'text': '{{liNo}}', 'matchCase': True},
                    'replaceText': f'{row['Pharmacy License Number']}'
                }
            },
            {
                'replaceAllText': {
                    'containsText': {'text': '{{res}}', 'matchCase': True},
                    'replaceText': f'{res}'
                }
            },
        ]

        docs_service.documents().batchUpdate(documentId=complaint_summary_id, body={'requests': requests}).execute()
        docs_service.documents().batchUpdate(documentId=notice_of_complaint_id, body={'requests': requests}).execute()


def pharm_clean(dds: pl.LazyFrame) -> None:
    """
    takes the proper action for the delinquent data submitters process based on the day of the week

    args:
        dds: the dds lazyframe returned by `process_input_files()`
    """
    today = datetime.now(tz=PHX_TZ).date()

    if today.weekday() == WEDNESDAY:  # notify compliance team of deadlines that fall in the next week
        deadlines = (
            drive.lazyframe_from_id_and_sheetname(os.environ['DDS_DEADLINES_FILE'], 'dds_deadlines', infer_schema_length=0)  # read_excel does not have infer_schema
            .cast({pl.Null: pl.String})
        )

        new_complaints = missed_deadlines_to_complaint(deadlines)
        if new_complaints is not None:
            generate_complaint_docs(new_complaints)

        due_next_week = date_in_next_week(deadlines).collect()
        if due_next_week.height > 0:
            msg = f'the following pharmacies have deadlines next week:\n{'\n'.join(f'permit: {item[0]} deadline: {item[1]}' for item in zip(due_next_week['Pharmacy License Number'].to_list(), due_next_week['deadline'].to_list(), strict=True))}\ncomplaints should be opened if the deadlines are missed\n\nthank you!'
        else:
            msg = 'no pharmacies have deadlines next week\n\nthank you!'
        dnw_msg = email.EmailMessage(
            sender=os.environ['EMAIL_COMPLIANCE'],
            to=os.environ['EMAIL_COMPLIANCE'],
            subject=f'DDS Pharmacies with Deadlines Next Week - {today.strftime('%Y-%m-%d')}',
            message_text=msg,
            monospace=True
        )
        email.send_email(dnw_msg, draft=(not args.send_emails))

    if today.weekday() == FRIDAY:  # add new pharmacies to the deadlines list and apply deadline
        due_date = num_and_dt.add_business_days(today)
        deadlines = (
            drive.lazyframe_from_id_and_sheetname(os.environ['DDS_DEADLINES_FILE'], 'dds_deadlines', infer_schema_length=0)  # read_excel does not have infer_schema
            .cast({pl.Null: pl.String})
        )

        new_complaints = missed_deadlines_to_complaint(deadlines)
        if new_complaints is not None:
            generate_complaint_docs(new_complaints)
            deadlines = (
                drive.lazyframe_from_id_and_sheetname(os.environ['DDS_DEADLINES_FILE'], 'dds_deadlines', infer_schema_length=0)  # read_excel does not have infer_schema
                .cast({pl.Null: pl.String})
            )

        new_deadlines = (
            dds
            .filter(
                (pl.col('Days Delinquent').str.to_integer() >= WEEKLY_DAYS_DELINQUENT_THRESHOLD) |
                (pl.col('Days Delinquent') == '') |  # noqa: PLC1901 | empty string is not falsey in polars
                (pl.col('Days Delinquent').is_null())
            )
            .join(deadlines, on='Pharmacy License Number', how='anti')
        )
        if new_deadlines.collect().height > 0:
            new_deadlines = (
                new_deadlines
                .drop('Days Delinquent')
                .with_columns(
                    pl.lit(due_date).dt.to_string('%Y-%m-%d').alias('deadline')
                )
            )
            deadlines = pl.concat([deadlines, new_deadlines])

        deadlines_path = Path('deadlines.csv')
        deadlines.collect().write_csv(deadlines_path)
        drive.update_sheet(deadlines_path, os.environ['DDS_DEADLINES_FILE'], 'dds_deadlines')
        deadlines_path.unlink()

        send_notices(deadlines, 'friday')
    else:
        send_notices(dds, 'daily')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='delinquent data submitters')
    parser.add_argument('-s', '--send-emails', action='store_true', help='send emails instead of creating drafts')
    args = parser.parse_args()
    load_dotenv()
    mp_path = Path('data/pharmacies.csv')
    files.warn_file_age(mp_path)

    dds_path = Path('data/DelinquentDispenserRequest.csv')
    files.warn_file_age(dds_path)

    lr_path = Path('data/List Request.csv')
    files.warn_file_age(lr_path)

    dds = process_input_files(mp_path, dds_path, lr_path)
    pharm_clean(dds)
