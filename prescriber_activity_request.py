import glob
import re
import sys
from datetime import datetime

import polars as pl
import pymupdf

from utils import tableau

print('finding luid for prescriber activity report...')
luid = tableau.find_view_luid('par', 'Prescriber Activity Report')
print(f'luid found: {luid}')

pdfs = glob.glob('data/*.pdf')
if pdfs:
    print('---')
    print(f'reading {len(pdfs)} pdfs...')
    for pdf in pdfs:
        page_text = pymupdf.get_text(pdf, pages=[1])[0]

        deas = re.findall(r'[A-Z]{2}[\d]{7}', page_text)
        date_range = re.findall(r'([\d]+/[\d]+/[\d]+) (?:-|through|to) ([\d]+/[\d]+/[\d]+)', page_text)
        start_date = datetime.strptime(date_range[0][0], '%m/%d/%Y').date()
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
            lf = tableau.lazyframe_from_view_id(luid, filters=filters, infer_schema_length=10000)

            # any polars stuff we want to do like renaming cols etc...

            file_name = f'{dea}_{start_date}_-_{end_date}'
            file_path = f'data/{file_name}.csv'
            df = lf.collect()
            if df.is_empty():
                msg_dict = {'message':f'no results found for {file_name}'}
                pl.DataFrame(msg_dict).write_csv(file_path)
                print(f'{file_path} was empty, empty file written')
            else:
                df.write_csv(file_path)
                print(f'{file_path} written')
else:
    sys.exit('no pdfs in data folder')

