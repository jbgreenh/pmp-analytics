import argparse
import os
import sys
import time
from calendar import FRIDAY, WEDNESDAY
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Literal

import polars as pl
from az_pmp_utils import auth, deas, drive, email, files, num_and_dt, tableau
from dotenv import load_dotenv
from googleapiclient import errors
from googleapiclient.discovery import build

from constants import MAX_DAYS_EXCUSED, MAX_MISSING_AGE, PHX_TZ

type EmailType = Literal['daily', 'friday']


class StaleVendorDataError(Exception):
    """exception for when vendor data is not up to date"""
    def __init__(self, message: str = 'stale vendor data') -> None:
        """initializes the error"""
        self.message = message
        super().__init__(self.message)


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
        res = 'resident' if row['state'] == 'AZ' else 'non-resident'
        address = f'{row['street_address']}\n{row['add2']}' if row['add2'] else row['street_address']

        complaint_summary_id = drive_service.files().copy(
            fileId=os.environ['DDS_COMPLAINT_SUMMARY_FILE'],
            body={
                'name': f'{row['pharmacy_name']} Complaint Summary',
                'parents': [row['folder_id']]
            }, supportsAllDrives=True
         ).execute()['id']

        notice_of_complaint_id = drive_service.files().copy(
            fileId=os.environ['DDS_NOTICE_OF_COMPLAINT_FILE'],
            body={
                'name': f'{row['pharmacy_name']} Notice of Complaint',
                'parents': [row['folder_id']]
            }, supportsAllDrives=True
         ).execute()['id']

        requests = [
            {
                'replaceAllText': {
                    'containsText': {'text': '{{bus_name}}', 'matchCase': True},
                    'replaceText': f'{row['pharmacy_name']}'
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
                    'replaceText': f'{row['city']}'
                }
            },
            {
                'replaceAllText': {
                    'containsText': {'text': '{{state}}', 'matchCase': True},
                    'replaceText': f'{row['state']}'
                }
            },
            {
                'replaceAllText': {
                    'containsText': {'text': '{{zip}}', 'matchCase': True},
                    'replaceText': f'{row['zip']}'
                }
            },
            {
                'replaceAllText': {
                    'containsText': {'text': '{{liNo}}', 'matchCase': True},
                    'replaceText': f'{row['permit_number']}'
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
            complaint_folder_id = drive.folder_id_from_name(folder_name=row['pharmacy_name'] + '-' + row['permit_number'], parent_folder_id=os.environ['PHARMACY_REPORTING_COMPLAINTS_FOLDER'], create=True)
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


def pull_files(start_date: date) -> None:
    """
    pulls the necessary files from tableau for running this script with `-r` or `--run`

    args:
        start_date: the first date to include in the files

    raises:
        StaleVendorDataError : an error for when the data on the tableau has not been updated recently enough
    """
    today = datetime.now(tz=PHX_TZ).date()
    two_days_ago = today - timedelta(days=2)

    print('getting luids...')
    sh_luid = tableau.find_view_luid('Submission History', 'Pharmacy Compliance')
    print(f'found submission history luid: {sh_luid}')
    prd_luid = tableau.find_view_luid('pharmacy_reported_dates', 'pharmacy_reported_dates')
    print(f'found pharmacy_reported_dates luid: {prd_luid}')

    print('pulling pharmacy report dates...')
    tstart = time.perf_counter()
    filters = {  # TODO: remove filled at from pharmacy_reported_dates between_dates calculated field when sold date becomes required
        'start_date': start_date,
        'end_date': two_days_ago,
    }
    pharmacy_report_dates = tableau.lazyframe_from_view_id(prd_luid, filters=filters, infer_schema=False).collect()
    fp = Path('data/pharmacy_report_dates.csv')
    pharmacy_report_dates.write_csv(fp)
    print(f'{fp} written: {(time.perf_counter() - tstart):2f}s')
    max_report_date = (
        pharmacy_report_dates
        .with_columns(
            pl.col('Day of Dispensations Created At').str.to_date('%B %d, %Y')
        )
        ['Day of Dispensations Created At']
        .max()
    )
    if max_report_date < (today - timedelta(days=1)):
        msg = f"""
the newest created date in the tableau dispensation source
is older than yesterday: {max_report_date}
(note that submission history may also be out of date)
check with the vendor to make sure this information is up-to-date
and rerun this script when the data is updated
        """
        raise StaleVendorDataError(msg)

    print('pulling submission history (this will take a while)...')
    tstart = time.perf_counter()
    sub_history = tableau.lazyframe_from_view_id(sh_luid, infer_schema=False).collect()

    fp = Path('data/submission_history.csv')
    sub_history.write_csv(fp)
    print(f'{fp} written: {(time.perf_counter() - tstart):2f}s')

    max_sub_date = (
        sub_history
        .with_columns(
            pl.col('Submission Date').str.to_datetime('%m/%d/%Y %I:%M:%S %p')
        )
        ['Submission Date']
        .max()
    )
    print()
    print('max submission datetime in tableau submission history:')
    print(max_sub_date.strftime('%m/%d/%Y %I:%M:%S %p'))
    print()

    if max_sub_date.date() < (today - timedelta(days=1)):
        msg = f"""
the newest submission date in Submission History
is older than yesterday: {max_sub_date}
check with the vendor to make sure this information is up-to-date
and rerun this script when the data is updated
        """
        raise StaleVendorDataError(msg)


def check_for_missing_zeros(start_date: date) -> pl.LazyFrame:
    """
    checks for pharmacies that have required days that are not covered by zero reports

    args:
        start_date: the first day to require

    returns:
        a lazyframe with missing_zeros
    """
    day_before_start_date = start_date - timedelta(days=1)
    mp_path = Path('data/pharmacies.csv')
    files.warn_file_age(mp_path)
    manage_pharmacies = (
        pl.scan_csv(mp_path, infer_schema=False)
        .select(
            pl.col('DEA').str.strip_chars().str.to_uppercase().alias('dea'),
            pl.col('Pharmacy License Number').str.strip_chars().str.to_uppercase().alias('permit_number'),
            pl.col('Pharmacy Name').alias('pharmacy_name'),
            pl.col('Reporting Requirements').alias('reporting_requirements'),
            pl.col('Pharmacist Email').str.to_lowercase().alias('mp_email'),
            pl.col('Phone Number').alias('mp_phone')
        )
        .filter(pl.col('reporting_requirements') != 'Exempt')
    )

    pharmacy_deas = (
        deas.deas('pharm', az=False)
        .select(
            pl.col('DEA Number').str.strip_chars().str.to_uppercase().alias('dea'),
            pl.col('Date of Original Registration').str.to_date('%Y%m%d', strict=False).alias('dea_reg_date')
        )
    )

    igov_path = Path('data/List Request.csv')
    files.warn_file_age(igov_path)
    igov_pharmacies = (
        pl.scan_csv(igov_path, infer_schema=False)
        .select(
            pl.col('License/Permit #').str.strip_chars().str.to_uppercase().alias('permit_number'),
            pl.col('Status').str.strip_chars().str.to_uppercase().alias('status'),
            pl.col('Type').alias('type'),
            pl.col('Issue Date').str.to_date('%m/%d/%Y').alias('igov_issue_date'),
            pl.col('Street Address').alias('street_address'),
            pl.col('Apt/Suite #').alias('add2'),
            pl.col('City').alias('city'),
            pl.col('State').alias('state'),
            pl.col('Zip').alias('zip'),
            pl.col('Email').str.to_lowercase().alias('igov_email'),
            pl.col('Phone').alias('igov_phone')
        )
        .filter(
            (pl.col('type') == 'Pharmacy') | (pl.col('type') == 'Remote Pharmacy')
        )
        .unique()  # this file has an entry for each PIC
    )

    weekday_map = {"monday": 1, "tuesday": 2, "wednesday": 3, "thursday": 4, "friday": 5, "saturday": 6, "sunday": 7}
    today = datetime.now(tz=PHX_TZ).date()
    two_days_ago = today - timedelta(days=2)

    # two_days_ago is actually 3 days ago until bamboo fixes their timing
    # remove this block if that ever happens
    # also note that exempt_days is not accounted for by this change
    # if a dispenser was not exempt yesterday,
    # they are still expected to have reported 3 days ago
    # which leads to very rare false positives:
    # if that dispenser reported late evening yesterday
    # but were exempt two_days_ago and 3 days ago
    two_days_ago -= timedelta(days=1)

    required_days = [start_date + timedelta(days=i) for i in range((two_days_ago - start_date).days + 1)]

    backwards_week_seq = [((today.weekday() - i - 1) % 7) + 1 for i in range(7)]
    backwards_date_seq = [(today - timedelta(days=1)) - timedelta(days=i) for i in range(7)]

    sh_path = Path('data/submission_history.csv')
    files.warn_file_age(sh_path)
    return (
        pl.scan_csv(sh_path, infer_schema=False).pipe(
            lambda lf: lf.rename({c: c.strip() for c in lf.collect_schema().names()})  # bamboo has random extra spaces after col names
        )
        .with_columns(pl.all().str.strip_chars())
        .select(
            pl.col('DEA Number').str.to_uppercase().alias('dea'),
            pl.col('Exempt Weekdays').str.to_lowercase().str.split(', ').alias('exempt_days'),
            pl.col('Submission Date').str.to_datetime('%m/%d/%Y %I:%M:%S %p').dt.replace_time_zone('America/Phoenix').alias('submission_date'),
            pl.col('Zero Report Start Date').str.to_date('%m/%d/%Y', strict=False).alias('zero_start_date'),
            pl.col('Zero Report End Date').str.to_date('%m/%d/%Y', strict=False).alias('zero_end_date'),
            'submitter',
            'submitter_email',
            'submitter_phone_number',
        )
        .filter(pl.col('submission_date') >= datetime(day_before_start_date.year, day_before_start_date.month, day_before_start_date.day, tzinfo=PHX_TZ))  # submission dates are in PHX_TZ but zero report dates do not have a timezone
        .join(manage_pharmacies, on='dea', how='right')                                                                                                    # meaning sometimes out of state submitters might have "submitted" zeros for today, yesterday
        .with_columns(
            pl.lit(required_days).alias('required_days'),
            pl.lit(backwards_date_seq).alias('backwards_date_seq'),
            pl.col('exempt_days').list.eval(
                pl.element().replace_strict(weekday_map, default=None).cast(pl.Int32)
            ).alias('exempt_nums')
        )
        .join(pharmacy_deas, on='dea', how='left')
        .join(igov_pharmacies, on='permit_number', how='left')
        .filter(pl.col('status').str.starts_with('OPEN'))
        .with_columns(pl.max_horizontal('dea_reg_date', 'igov_issue_date').alias('max_issue_date'))
        .with_columns(
            pl.date_ranges(start=pl.col('zero_start_date'), end=pl.col('zero_end_date'), interval='1d').alias('zero_report_days'),
            pl.date_ranges(start=pl.col('required_days').list.first(), end=pl.col('max_issue_date') - pl.duration(days=1), interval='1d').fill_null([]).alias('days_to_remove'),
            pl.concat_list([
                pl.col('exempt_nums').list.contains(wd) for wd in backwards_week_seq
            ]).alias('required_bool_list')
        )
        .with_columns(
            pl.col('required_days').list.set_difference('days_to_remove')
        )
        .with_columns(
            pl.when((pl.col('required_bool_list').list.arg_min() > 0) & (pl.col('required_bool_list').list.arg_min() <= MAX_DAYS_EXCUSED))
            .then(pl.col('required_bool_list').list.arg_min() + 1)
            .otherwise(0)
            .alias('k')
        )
        .with_columns(
            pl.when(pl.col('required_bool_list').list.get(0))
            .then(pl.col('backwards_date_seq').list.head(pl.col('k')))
            .otherwise(pl.lit([]))
            .alias('exempt_dates')
        )
        .with_columns(
            pl.col('required_days').list.set_difference('exempt_dates'),
        )
        .group_by(['dea', 'permit_number', 'pharmacy_name', 'status', 'street_address', 'add2', 'city', 'state', 'zip', 'mp_email', 'igov_email', 'mp_phone', 'igov_phone', 'max_issue_date', 'required_days', 'exempt_days'])
        .agg(
            pl.col(['zero_report_days', 'submitter_email', 'submitter_phone_number']).list.explode(empty_as_null=False, keep_nulls=False).unique().sort(),
        )
        .with_columns(
            pl.col('required_days').list.set_difference('zero_report_days').alias('missing_zeros'),
            pl.concat_list(pl.col('submitter_email'), pl.col('mp_email'), pl.col('igov_email')).list.unique().list.join(',').alias('to'),
            pl.concat_list(pl.col('submitter_phone_number'), pl.col('mp_phone'), pl.col('igov_phone')).list.unique().list.join(',').alias('phone_numbers'),
        )
        .drop('submitter_email', 'mp_email', 'igov_email', 'submitter_phone_number', 'mp_phone', 'igov_phone')
    )


def check_missing_zeros_for_missing_sold(missing_zeros: pl.LazyFrame, report_dates: pl.LazyFrame) -> pl.LazyFrame:
    """
    accounts for reported sold dates for pharmacies that did not have zero reports covering required days

    args:
        missing_zeros: lazyframe with all pharmacies that did not have zero reports covering required days
        report_dates: lazyframe with reported dispensations

    returns:
        a lazyframe with missing_sold_and_zero
    """
    return (
        missing_zeros
        .join(report_dates, on='dea', how='left')
        .with_columns(
            pl.col('missing_zeros').list.set_difference(pl.col('sold_date').fill_null([])).list.sort().alias('missing_sold_and_zero')
        )
        .select('dea', 'permit_number', 'pharmacy_name', 'status', 'to', 'phone_numbers', 'street_address', 'add2', 'city', 'state', 'zip', 'max_issue_date', 'exempt_days', 'missing_sold_and_zero')
        .filter(pl.col('missing_sold_and_zero').list.len() > 0)
    )


def check_missing_zero_sold_for_missing_filled(missing_sold_zero: pl.LazyFrame, report_dates: pl.LazyFrame) -> pl.LazyFrame:  # TODO: remove when sold date is required
    """
    checks the provided list for filled dates that cover the previous missing dates

    args:
      missing_sold_zero: a lazyframe with the dates that have neither a sold date or zero report in col('missing_sold_and_zero')
      report_dates: lazyframe with reported dispensations

    returns:
        a lazyframe with missing_sold_filled_zero
    """
    return (
        missing_sold_zero
        .join(report_dates, on='dea', how='left')
        .with_columns(
            pl.col('missing_sold_and_zero').list.set_difference(pl.col('filled_date').fill_null([])).list.sort().alias('missing_sold_filled_zero')
        )
        .select('dea', 'permit_number', 'pharmacy_name', 'status', 'to', 'phone_numbers', 'street_address', 'add2', 'city', 'state', 'zip', 'max_issue_date', 'exempt_days', 'missing_sold_and_zero', 'missing_sold_filled_zero')
        .filter(pl.col('missing_sold_filled_zero').list.len() > 0)
    )


def final_missing(missing: pl.LazyFrame, start_date: date) -> pl.LazyFrame:
    """
    final processing and uploading of missing_dates report

    args:
        missing: list of pharmacies with missing dates
        start_date: the first day reporting is required as specified with `-s` or `--start-date`

    returns:
        a lazyframe of dds recipients
    """
    submission_deas = (
        pl.scan_csv('data/submission_history.csv', infer_schema=False)
        .collect()
        ['DEA Number'].unique()
        .to_list()
    )

    missing_csv = (
        missing
        .with_columns(pl.col('missing_sold_filled_zero').list.len().alias('count_missing'))  # TODO: use missing_sold_and_zero when sold date is required
        .with_columns(pl.col('exempt_days').list.join(', '))
        .with_columns(pl.col(['missing_sold_and_zero', 'missing_sold_filled_zero']).list.eval(pl.element().dt.strftime('%Y-%m-%d')).list.join(', '))  # TODO: remove missing_sold_filled_zero when sold date becomes required
        .with_columns(
            pl.when(pl.col('dea').is_in(submission_deas).not_())
            .then(pl.lit('never submitted'))
            .otherwise(pl.col('count_missing'))
            .alias('count_missing')
        )
        .drop('missing_sold_and_zero')  # TODO: adjust for when sold date is required
        .rename({'missing_sold_filled_zero': 'missing_dates'})  # TODO: adjust for when sold date is required
        .select(
            'dea',
            'permit_number',
            'pharmacy_name',
            'status',
            'exempt_days',
            'max_issue_date',
            'missing_dates',
            'count_missing',
            'to',
            'phone_numbers',
            'street_address',
            'add2',
            'city',
            'state',
            'zip',
        )
        .sort(pl.col('missing_dates').str.len_chars(), descending=True)
    )

    today = datetime.now(tz=PHX_TZ).date()
    fp = Path(f'data/{today.strftime('%Y%m%d')}-missing-reports-starting-{start_date.strftime('%Y%m%d')}.csv')
    missing_csv.collect().write_csv(fp)
    print('================================')
    drive.upload_csv_as_sheet(fp, os.environ['DDS_MISSING_REPORTS_FOLDER'])
    print('================================')
    fp.unlink()

    return missing_csv


def remove_pharmacies_with_active_complaints(dds: pl.LazyFrame) -> pl.LazyFrame:
    """
    remove pharmacies with active complaints from the dds list

    args:
        dds: the list of dds recipients

    returns:
        the filtered lazyframe
    """
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

    return dds.join(complaints, left_on='permit_number', right_on='Pharmacy License Number', how='anti')


def send_notices(lf: pl.LazyFrame, email_type: EmailType) -> None:
    """
    send dds email notices (or create drafts)

    args:
        lf: a lazyframe with the dds recipients
        email_type: daily or friday notices
    """
    service = build('gmail', 'v1', credentials=auth.auth())
    timestamps = []
    notices = lf.collect()

    if args.send_emails:
        sanity_check = input(f'{notices.height} notices to be sent, does this make sense? (y/n): ')
        if sanity_check != 'y':
            sys.exit('no notices sent, verify data with vendor')

    for row in notices.iter_rows(named=True):
        pharmacy_address = f'{row['street_address']}, {row['add2']}\n{row['city']}, {row['state']} {row['zip']}' if row['add2'] else f'{row['street_address']}\n{row['city']}, {row['state']} {row['zip']}'
        if row['missing_dates'] == 'never submitted':
            missing_dates = 'no data has ever been received'
        else:
            missing_dates = row['missing_dates']

        if email_type == 'friday':
            subject = f'CSPMP Action Required: Possible Complaint Against {row['permit_number']}'
            body = f"""ATTENTION: {row['pharmacy_name']}
{pharmacy_address}
{row['permit_number']}
{row['dea']}

According to our records, your pharmacy is <b>not submitting daily</b> reports to the Arizona Controlled Substance Prescription Monitoring Program clearinghouse.

Dispensation data is missing for: <b>{missing_dates}</b>

At this time, you are in violation of <a href="https://www.azleg.gov/ars/36/02608.htm" target="_blank">ARS § 36-2608</a> reporting requirements. <em><b>Failure to upload your delinquent schedule II-V dispensations will result in a complaint being opened against the pharmacy.</b></em>

<b>Zero reports should be submitted for any days there are no controlled substance dispensations. For days you are not operational, you should report zero for those days on your next open business day.</b>

<b style='color: red;'>Please ensure you upload your missed submissions by {row['deadline']} or a complaint will be opened against the pharmacy permit. </b>

If your pharmacy has an active DEA number, an active AZ pharmacy permit, and is not limited to veterinary dispensing, <em><b>it is required to submit a daily report, including zero reports, for controlled substances II-V.</b></em>

If your pharmacy utilizes a vendor to submit dispensations on your behalf, please contact your vendor immediately to get this issue corrected to avoid possible Board action. You may forward this email to the appropriate vendor contact for assistance.

If you are receiving this message and you are a data vendor reporting submissions on behalf of the pharmacy, please be aware that the pharmacy is delinquent in AZ CSPMP reporting and faces possible Board action if not corrected by the given deadline. Please forward this information to the appropriate members of the pharmacy team if necessary.

If you have any questions or concerns about the data submission process, please contact Bamboo Health for Technical Support directly at 1-855-929-4767. Technical Support is available 24 hours a day, 7 days a week.

<a href="https://drive.google.com/file/d/1R1wCymw9T5n2sqn8fQGuWeEoCChXmjB0/view?ts=67ca02cd" target="_blank">AZ Data Submission Dispenser Guide</a>
<a href="https://pharmacypmp.az.gov/data-submissions-faqs" target="_blank">AZ Data Submission FAQs</a>{os.environ['EMAIL_COMP_SIG'].replace(r'\n', '\n')}
            """
        else:
            subject = f'Notice of Missing CSPMP Data Submissions for {row['permit_number']}'
            body = f"""<b>At this time, your pharmacy, {row['permit_number']}, is in violation of <a href="https://www.azleg.gov/ars/36/02608.htm" target="_blank">ARS § 36-2608</a> reporting requirements.</b>

You are receiving this email because you are listed as the party responsible for submitting controlled substance dispensing information for the above-referenced dispenser to the Arizona Controlled Substances Prescription Monitoring Program (AZ CSPMP).

Controlled substance dispensing information is missing for: <b>{missing_dates}</b>.

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
            print(f'failed to send message for {row['permit_number']} | {row['dea']}:')
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
            'permit_number',
            'dea',
            'missing_dates',
            'zip',
            pl.lit(email_type).alias('email_type')
        )
    )
    full_logs = pl.concat([logs, new_dds_log])
    fl_path = Path('full_logs.csv')
    full_logs.write_csv(fl_path)
    drive.update_sheet(fl_path, os.environ['DDS_EMAIL_LOGS_FILE'], sheet_name='dds_email_logs')
    fl_path.unlink()


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
            msg = f'the following pharmacies have deadlines next week:\n{'\n'.join(f'permit: {item[0]} deadline: {item[1]}' for item in zip(due_next_week['permit_number'].to_list(), due_next_week['deadline'].to_list(), strict=True))}\ncomplaints should be opened if the deadlines are missed\n\nthank you!'
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
                pl.col('missing_dates').str.split(',').list.eval(pl.element().str.to_date('%Y-%m-%d')).list.min() < (today - timedelta(days=MAX_MISSING_AGE))  # TODO: decide on the actual age for this filter/any other filters to add
            )
            .join(deadlines, on='permit_number', how='anti')
        )

        if new_deadlines.collect().height > 0:
            new_deadlines = (
                new_deadlines
                .with_columns(
                    pl.lit(due_date).dt.to_string('%Y-%m-%d').alias('deadline')
                )
            )
            deadlines = pl.concat([deadlines, new_deadlines])

        deadlines_path = Path('deadlines.csv')
        deadlines.collect().write_csv(deadlines_path)
        drive.update_sheet(deadlines_path, os.environ['DDS_DEADLINES_FILE'], 'dds_deadlines')
        deadlines_path.unlink()

        dds_not_in_deadlines = (
            dds
            .join(deadlines, on='permit_number', how='anti')
        )

        send_notices(dds_not_in_deadlines, 'daily')
        send_notices(deadlines, 'friday')
    else:
        send_notices(dds, 'daily')


if __name__ == '__main__':
    load_dotenv()

    parser = argparse.ArgumentParser(description='delinquent data submitters')
    parser.add_argument('-ts', '--tableau-start-date', type=date.fromisoformat, default=date(2006, 6, 9), help='start date for tableau in YYYY-MM-DD format (default: 30 days ago)')
    parser.add_argument('-rs', '--required-start-date', type=date.fromisoformat, default=date(2006, 6, 9), help='start date for data submission requirement in YYYY-MM-DD format (default: 30 days ago)')
    parser.add_argument('-p', '--pull-files', action='store_true', help='pull files from tableau and exit')
    parser.add_argument('-r', '--run', action='store_true', help='run and write data/missing_sold_and_zero.csv')
    parser.add_argument('-s', '--send-emails', action='store_true', help='send emails instead of creating drafts when using --run')
    args = parser.parse_args()

    today = datetime.now(tz=PHX_TZ).date()
    thirty_one_days_ago = today - timedelta(days=31)
    tab_start_date = max(args.tableau_start_date, thirty_one_days_ago)
    req_start_date = max(args.required_start_date, thirty_one_days_ago)

    if args.pull_files:
        pull_files(tab_start_date)

    if args.run:
        # TODO: move this paragraph to check_missing_zeros_for_missing_sold when sold date becomes required
        # and remove filled date stuff
        pharmacy_report_dates_fp = Path('data/pharmacy_report_dates.csv')
        files.warn_file_age(pharmacy_report_dates_fp)
        report_dates = (
            pl.scan_csv(pharmacy_report_dates_fp, infer_schema=False)
            .select(
                pl.col('Pharmacy DEA').str.strip_chars().str.to_uppercase().alias('dea'),
                pl.col('Day of Dispensations Created At').str.to_date('%B %d, %Y').alias('create_date'),
                pl.col('Day of Written At').str.to_date('%B %d, %Y').alias('written_date'),
                pl.col('Day of Filled At').str.to_date('%B %d, %Y').alias('filled_date'),
                pl.col('Day of Dispensations Sold At').str.to_date('%B %d, %Y').alias('sold_date'),
            )
            .group_by('dea')
            .agg(['create_date', 'written_date', 'filled_date', 'sold_date'])
        )

        missing_zeros = check_for_missing_zeros(req_start_date)
        missing = check_missing_zeros_for_missing_sold(missing_zeros, report_dates)  # TODO: remove report_dates param and integrate into the function when sold date is required
        missing = check_missing_zero_sold_for_missing_filled(missing, report_dates)  # TODO: remove when sold date is required
        dds = final_missing(missing, req_start_date)
        dds = remove_pharmacies_with_active_complaints(dds)
        pharm_clean(dds)
