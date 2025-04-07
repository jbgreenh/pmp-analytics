import argparse
import glob
import re
import sys
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

from dotenv import load_dotenv
import polars as pl
import pymupdf

from utils import tableau

def activity_request(request_type:str):
    print(f'finding luid for {request_type} activity report...')
    luid = tableau.find_view_luid(f'{request_type}_activity_request', 'DEA Records Request')
    print(f'luid found: {luid}')

    log_fp = 'data/activity_request_log.txt'
    with open(log_fp, 'w') as file:
        pass # clear logs

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
            date_range = re.findall(r'(\d{1,2}/\d{1,2}/\d{4})\s*(?:-|through|to)\s*(\d{1,2}/\d{1,2}/\d{4})', page_text)
            if not date_range:
                print('---')
                print(f'could not find a daterange in {pdf}')
                print(f'see {log_fp}')
                with open(log_fp, 'a') as file:
                    file.write(f'---\ncould not find a daterange in {pdf}\n:::\npage text:\n:::\n{page_text}\n---')
                continue
            start_date = datetime.strptime(date_range[0][0], '%m/%d/%Y').date()
            seven_years_ago = date.today() - relativedelta(years=7)

            if start_date < seven_years_ago:
                start_date = seven_years_ago
            end_date = datetime.strptime(date_range[0][1], '%m/%d/%Y').date()

            print('---')
            print(pdf)
            print(deas)
            print(f'{start_date} - {end_date}')
            print('\n')

            for dea in deas:
                filters = {
                    'dea':dea, 'start_date':start_date, 'end_date':end_date
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

                file_name = f'{dea}_{start_date}_-_{end_date}'
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
    else:
        sys.exit(f'no pdfs in data/{request_type}/ folder')

def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description='pull a request')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-p', '--prescriber', action='store_true', help='pull prescriber activity request')
    group.add_argument('-d', '--dispenser', action='store_true', help='pull dispenser activity request')
    args = parser.parse_args()
    if args.prescriber:
       activity_request('prescriber')
    elif args.dispenser:
       activity_request('dispenser')

if __name__ == '__main__':
    main()
