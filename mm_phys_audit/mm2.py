import datetime
import polars as pl
import toml
from googleapiclient.discovery import build

from utils import drive, auth

def mm2(service):
    # read and combine files for six months of lookups
    year = datetime.datetime.now().year
    month = datetime.datetime.now().month
    start, end = 0, 0
    if month < 7:
        year = year - 1
        start, end = 7, 12
    else:
        start, end = 1, 6

    lookups = pl.LazyFrame()
    for n in range(start, end+1):
        n = str(n).zfill(2)
        print(f'{year}{n}:')
        requests_folder_id = secrets['folders']['patient_requests']
        requests_folder_id = drive.folder_id_from_name(service=service, folder_name=f'AZ_PtReqByProfile_{year}{n}', parent_id=requests_folder_id)
        if requests_folder_id:
            requests = drive.lazyframe_from_file_name_csv(service=service, file_name='Prescriber.csv', folder_id=requests_folder_id, separator='|', infer_schema_length=10000)

        lookups = pl.concat([lookups, requests])

    # group by DEA Number and sum of totallookups
    lookups = (
        lookups.collect().lazy() # ??????????????????
        .select('dea_number','totallookups')
        .group_by('dea_number')
        .agg(pl.col('totallookups').sum())
    )

    mm_matches = (
        pl.scan_csv('data/mm_matches_combined.csv', infer_schema_length=10000)
        .with_columns(
            pl.lit('').alias('note')
        )
    )
    mm_manual = (
        pl.scan_csv('data/mm_manual.csv', infer_schema_length=10000)
    )

    mm_combined = pl.concat([mm_matches, mm_manual])
    mm_combined = (
        mm_combined
        .join(lookups, left_on='DEA Number', right_on='dea_number', how='left', coalesce=True)
        .with_columns(
            pl.col('totallookups').fill_null(0)
        )
        .with_columns(  # extra with_columns to force fill_null(0) first
            (pl.col('totallookups') / pl.col('Application Count')).alias('Lookups/Count'),
            (pl.col('Application Count') >= 20).alias('>=20'),
            ((pl.col('totallookups') / pl.col('Application Count')) < 0.8).alias('<80% Lookups')
        )
        .with_columns(  # extra with_columns to force >=20 and <80% Lookups to be created first
            (pl.col('>=20') & pl.col('<80% Lookups')).alias('test')
        )
        .sort(['test', 'Application Count'], descending=[True, True])
    )

    file_path = 'data/mmq.xlsx'
    mm_combined.collect().write_excel(
        file_path,
        worksheet='mm phys audit',
        conditional_formats={       # something is making giving the columns as a tuple act strange
            '>=20':[{
                'type':'cell',
                'criteria':'equal to',
                'value':'TRUE',
                'format': {'bg_color':'#F4CCCC'}
            },{
                'type':'cell',
                'criteria':'equal to',
                'value':'FALSE',
                'format': {'bg_color':'#D9EAD3'}
            }],
            '<80% Lookups':[{
                'type':'cell',
                'criteria':'equal to',
                'value':'TRUE',
                'format': {'bg_color':'#F4CCCC'}
            },{
                'type':'cell',
                'criteria':'equal to',
                'value':'FALSE',
                'format': {'bg_color':'#D9EAD3'}
            }],
            'test':[{
                'type':'cell',
                'criteria':'equal to',
                'value':'TRUE',
                'format': {'bg_color':'#F4CCCC'}
            },{
                'type':'cell',
                'criteria':'equal to',
                'value':'FALSE',
                'format': {'bg_color':'#D9EAD3'}
            }],
        },
        autofit=True,
        freeze_panes='A2'
    )
    print(f'{file_path} saved')


if __name__ == '__main__':
    with open('../secrets.toml', 'r') as f:
        secrets = toml.load(f)
    creds = auth.auth()
    service = build('drive', 'v3', credentials=creds)
    mm2(service)
