import argparse
import glob
import re
import sys
from dataclasses import dataclass
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

from dotenv import load_dotenv
import polars as pl
import pymupdf

from utils import tableau

@dataclass
class SearchParameters:
    """
    class with search parameters for pulling tableau files
    
    attributes:
        `deas`: a list of dea numbers to search for
        `start_date`: start date for search
        `end_date`: end date for search
        ``
    """
    deas: list[int]
    start_date: date
    end_date:date

def process_pdf(request_type) -> SearchParameters:
    log_fp = 'data/activity_request_log.txt'
    with open(log_fp, 'w') as file:
        pass # clear logs

    if request_type == 'audit_trail':
        pdfs = glob.glob(f'data/{request_type}/*.pdf')
    else:
        pdfs = glob.glob(f'data/{request_type}_activity_request/*.pdf')

    if pdfs:
        print('---')
        print(f'reading {len(pdfs)} {"pdf" if len(pdfs) == 1 else "pdfs"}...')
        for pdf in pdfs:
            page = pymupdf.open(pdf).load_page(0)
            page_text = page.get_text()
            if not page_text:
                print('---')
                print(f'{pdf} does not have readable text')
                print('attempting ocr...')
                pdfocr = page.get_pixmap(matrix=pymupdf.Matrix(2,2)).pdfocr_tobytes()
                page_text = pymupdf.open('pdf', pdfocr).load_page(0).get_text()
                if not page_text:
                    print(f'{pdf} could not be read through ocr')
                    continue
            deas = re.findall(r'[A-Z]{2}\d{7}', page_text)
            if not deas:
                print('---')
                print(f'could not find any deas in {pdf}')
                print(f'see {log_fp}')
                with open(log_fp, 'a') as file:
                    file.write(f'---\ncould not find any deas in {pdf}\n:::\npage text:\n:::\n{page_text}\n---')
                continue
            date_range = re.findall(r'(\d{1,2}/\d{1,2}/(?:\d{4}|\d{2}))\s*(?:-|through|to)\s*(\d{1,2}/\d{1,2}/(?:\d{4}|\d{2}))', page_text)
            if not date_range:
                print('---')
                print(f'could not find a daterange in {pdf}')
                print(f'see {log_fp}')
                with open(log_fp, 'a') as file:
                    file.write(f'---\ncould not find a daterange in {pdf}\n:::\npage text:\n:::\n{page_text}\n---')
                continue
            if len(re.split('/', date_range[0][0])[2]) == 2:
                start_date = datetime.strptime(date_range[0][0], '%m/%d/%y').date()
            else:
                start_date = datetime.strptime(date_range[0][0], '%m/%d/%Y').date()

            seven_years_ago = date.today() - relativedelta(years=7)
            if start_date < seven_years_ago:
                start_date = seven_years_ago

            if len(re.split('/', date_range[0][1])[2]) == 2:
                end_date = datetime.strptime(date_range[0][1], '%m/%d/%y').date()
            else:
                end_date = datetime.strptime(date_range[0][1], '%m/%d/%Y').date()

            print('---')
            print(pdf)
            print(deas)
            print(f'{start_date} - {end_date}')
            print('\n')
            return SearchParameters(deas, start_date, end_date)
    else:
        sys.exit(f'no pdfs in data/{request_type}/ folder')

def activity_request(request_type:str, params:SearchParameters):
    if request_type == 'audit_trail':
        workbook_name = f'dea_{request_type}'
        print(f'finding luid for {workbook_name} report...')
        user_ids_luid = tableau.find_view_luid('UserIDs', workbook_name)
        print(f'luid found: {user_ids_luid}')
        searches_luid = tableau.find_view_luid('Searches', workbook_name)
        print(f'luid found: {searches_luid}')
        users_luid = tableau.find_view_luid('users', workbook_name)
        print(f'luid found: {users_luid}')

        print('pulling users file...')
        users_lf = tableau.lazyframe_from_view_id(users_luid, infer_schema=False)

        user_ids = []
        for dea in params.deas:
            filters = {
                'search_dea':dea
            }

            print(f'pulling userids file for {dea}...')
            user_ids_df = tableau.lazyframe_from_view_id(user_ids_luid, filters, infer_schema=False).collect()

            if user_ids_df.height > 1:
                user_ids_df = user_ids_df.filter(pl.col('Active') == 'Y')
                if user_ids_df.height < 1:
                    sys.exit(f'{dea} has multiple associated user ids, but none are active')
                elif user_ids_df.height > 1:
                    sys.exit(f'{dea} has multiple associated active user ids')

            user_ids.append(user_ids_df['User ID'].first())

        for id in set(user_ids):
            filters = {
                'search_trueid':id, 'search_start_date':params.start_date, 'search_end_date':params.end_date
            }
            user_name = users_lf.filter(pl.col('User ID') == id).collect()['User Full Name'].first()

            print(f'pulling searches for {id}...')
            searches_lf = (
                tableau.lazyframe_from_view_id(searches_luid, filters, infer_schema=False)
                .join(users_lf, how='left', left_on='Requestor ID', right_on='User ID', coalesce=True)
                .drop('Requestor ID')
                .select(
                    'Search ID',
                    'Searched First Name',
                    'Searched Last Name',
                    pl.col('Month, Day, Year of Searched DOB').str.to_date('%B %d, %Y').alias('Searched DOB'),
                    'User Full Name',
                    'User Role',
                    'delegate?',
                    'Is Gateway Request?',
                    'Request Status',
                    pl.col('Month, Day, Year of Search Creation Date').str.to_date('%B %d, %Y').alias('Search Creation Date'),
                )
                .sort('Search Creation Date')
            )
            fn = f'data/{request_type}/{user_name}_audit_trail_{params.start_date}_-_{params.end_date}.csv'
            searches_lf.collect().write_csv(fn)
            print(f'{fn} written')

    else: # prescriber or dispenser activity request
        print(f'finding luid for {request_type} activity report...')
        luid = tableau.find_view_luid(f'{request_type}_activity_request', 'DEA Records Request')
        print(f'luid found: {luid}')

        for dea in params.deas:
            filters = {
                'dea':dea, 'start_date':params.start_date, 'end_date':params.end_date
            }
            if request_type == 'prescriber':
                lf = (
                    tableau.lazyframe_from_view_id(luid, filters=filters, infer_schema=False)
                    .select(
                        pl.col('Prescriber DEA'),
                        pl.col('Prescriber NPI'),
                        pl.col('Prescriber First Name'),
                        pl.col('Prescriber Last Name'),
                        pl.col('Orig Prescriber Address Line One').alias('Prescriber Address'),
                        pl.col('Orig Prescriber Address Line Two').alias('Prescriber Address 2'),
                        pl.col('Orig Prescriber City').alias('Prescriber City'),
                        pl.col('Orig Prescriber State Abbr').alias('Prescriber State'),
                        pl.col('Orig Prescriber Zip').alias('Prescriber ZIP'),
                        pl.col('Pharmacy DEA'),
                        pl.col('Pharmacy NPI'),
                        pl.col('Pharmacy Retail Name').alias('Pharmacy Name'),
                        pl.col('Orig Pharmacy Address Line One').alias('Pharmacy Address'),
                        pl.col('Orig Pharmacy Address Line Two').alias('Pharmacy Address 2'),
                        pl.col('Orig Pharmacy City').alias('Pharmacy City'),
                        pl.col('Orig Pharmacy State Abbr').alias('Pharmacy State'),
                        pl.col('Orig Pharmacy Zip').alias('Pharmacy ZIP'),
                        pl.col('Pharmacy Chain Site Id'),
                        pl.col('Prescription Number').alias('Rx Number'),
                        pl.col('Day of Filled At').alias('Rx Fill Date'),
                        pl.col('Day of Written At').alias('Rx Written Date'),
                        pl.col('Refills Authorized').alias('Authorized Refills'),
                        pl.col('Refill Y/N'),
                        pl.col('Payment Type'),
                        pl.col('Drug Schedule'),
                        pl.col('AHFS Description').alias('AHFS Drug Category'),
                        pl.col('Brand Name').alias('Drug Name'),
                        pl.col('Quantity'),
                        pl.col('Strength'),
                        pl.col('Dosage Type'),
                        pl.col('Days Supply'),
                        pl.col('Daily MME'),
                        pl.col('Patient First Name'),
                        pl.col('Patient Last Name'),
                        pl.col('Day of Patient Birthdate').alias('Patient DOB'),
                        pl.col('Orig Patient Address Line One').alias('Patient Address'),
                        pl.col('Orig Patient Address Line Two').alias('Patient Address 2'),
                        pl.col('Orig Patient City').alias('Patient City'),
                        pl.col('Orig Patient State Abbr').alias('Patient State'),
                        pl.col('Orig Patient Zip').alias('Patient ZIP'),
                        pl.col('Veterinarian Prescription Y/N')
                    )
                )
            else:   # dispenser
                lf = (
                    tableau.lazyframe_from_view_id(luid, filters=filters, infer_schema=False)
                    .select(
                        pl.col('Pharmacy DEA'),
                        pl.col('Pharmacy NPI'),
                        pl.col('Pharmacy Retail Name').alias('Pharmacy Name'),
                        pl.col('Orig Pharmacy Address Line One').alias('Pharmacy Address'),
                        pl.col('Orig Pharmacy Address Line Two').alias('Pharmacy Address 2'),
                        pl.col('Orig Pharmacy City').alias('Pharmacy City'),
                        pl.col('Orig Pharmacy State Abbr').alias('Pharmacy State'),
                        pl.col('Orig Pharmacy Zip').alias('Pharmacy ZIP'),
                        pl.col('Pharmacy Chain Site Id'),
                        pl.col('Patient First Name'),
                        pl.col('Patient Last Name'),
                        pl.col('Day of Patient Birthdate').alias('Patient DOB'),
                        pl.col('Orig Patient Address Line One').alias('Patient Address'),
                        pl.col('Orig Patient Address Line Two').alias('Patient Address 2'),
                        pl.col('Orig Patient City').alias('Patient City'),
                        pl.col('Orig Patient State Abbr').alias('Patient State'),
                        pl.col('Orig Patient Zip').alias('Patient ZIP'),
                        pl.col('Prescription Number').alias('Rx Number'),
                        pl.col('Day of Filled At').alias('Rx Fill Date'),
                        pl.col('Day of Written At').alias('Rx Written Date'),
                        pl.col('Refills Authorized').alias('Authorized Refills'),
                        pl.col('Refill Y/N'),
                        pl.col('Payment Type'),
                        pl.col('Drug Schedule'),
                        pl.col('AHFS Description').alias('AHFS Drug Category'),
                        pl.col('Brand Name').alias('Drug Name'),
                        pl.col('Quantity'),
                        pl.col('Strength'),
                        pl.col('Dosage Type'),
                        pl.col('Days Supply'),
                        pl.col('Daily MME'),
                        pl.col('Prescriber DEA'),
                        pl.col('Prescriber NPI'),
                        pl.col('Prescriber First Name'),
                        pl.col('Prescriber Last Name'),
                        pl.col('Orig Prescriber Address Line One').alias('Prescriber Address'),
                        pl.col('Orig Prescriber Address Line Two').alias('Prescriber Address 2'),
                        pl.col('Orig Prescriber City').alias('Prescriber City'),
                        pl.col('Orig Prescriber State Abbr').alias('Prescriber State'),
                        pl.col('Orig Prescriber Zip').alias('Prescriber ZIP'),
                        pl.col('Veterinarian Prescription Y/N'))
                    )

            file_name = f'{dea}_{params.start_date}_-_{params.end_date}'
            file_path = f'data/{request_type}_activity_request/{file_name}.csv'
            try:
                df = lf.collect()
            except pl.exceptions.NoDataError:
                msg_dict = {'message':f'no results found for {file_name}'}
                pl.DataFrame(msg_dict).write_csv(file_path)
                print(f'{file_path} was empty, empty file written')
            except Exception as e:
                print(e)
            else:
                df.write_csv(file_path)
                print(f'{file_path} written')

def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description='pull a request')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-p', '--prescriber', action='store_true', help='pull prescriber activity request')
    group.add_argument('-d', '--dispenser', action='store_true', help='pull dispenser activity request')
    group.add_argument('-at', '--audit-trail', action ='store_true', help='pull audit trail')
    args = parser.parse_args()
    if args.prescriber:
        params = process_pdf('prescriber')
        activity_request('prescriber', params)
    elif args.dispenser:
        params = process_pdf('dispenser')
        activity_request('dispenser', params)
    elif args.audit_trail:
        params = process_pdf('audit_trail')
        activity_request('audit_trail', params)

if __name__ == '__main__':
    main()
