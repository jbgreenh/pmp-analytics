import polars as pl
from datetime import date
import os
import toml

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from utils.auth import *

def pharm_clean():
    '''shape data for final report'''
    # get today's date as a string
    today = date.today().strftime("%m-%d-%Y")

    mp = (
        pl.scan_csv('data/pharmacies.csv')
        .with_columns(
            pl.col('DEA').str.strip_chars().str.to_uppercase()
        )
        .select('DEA', 'Pharmacy License Number')
    )

    igov = (
        pl.scan_csv('data/List Request.csv', infer_schema_length=10000)
        .with_columns(
            pl.col('License/Permit #').str.strip_chars().str.to_uppercase()
        )
        .rename(
            {'License/Permit #':'Pharmacy License Number'}
        )
        .select(
            'Pharmacy License Number', 'Status', 'Business Name', 'Street Address', 'Apt/Suite #', 
            'City', 'State', 'Zip', 'Email', 'Phone'
        )
    )

    ddr = (
        pl.scan_csv('data/DelinquentDispenserRequest.csv')
        .filter((pl.col('Days Delinquent') >= 14) | (pl.col('Days Delinquent').is_null()))
        .with_columns(
            pl.col('DEA').str.strip_chars().str.to_uppercase()
        )
        .join(mp, on='DEA', how='left')
        .with_columns(
            pl.col('Pharmacy License Number').str.strip_chars().str.to_uppercase()
        )
        .join(igov, on='Pharmacy License Number', how='left')
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
    
    if closed.shape[0] > 0:
        print('closed pharmacies, update in manage pharmacies in awarxe:')
        print(closed)
    else:
        print('no closed pharmacies')

    fname = f'{today}.csv'
    ddr.collect().write_csv(fname)
    return today, fname

def main():
    creds = auth()
    with open('../secrets.toml', 'r') as f:
        secrets = toml.load(f)
    try:
        today, fname = pharm_clean()

        service = build('drive', 'v3', credentials=creds)

        folder_id = secrets['folders']['pharm_clean']
        
        file_metadata = {
            'name': today, 
            'parents':[folder_id],
            'mimeType': 'application/vnd.google-apps.spreadsheet'
        }

        media = MediaFileUpload(fname,
                                mimetype='text/csv')
        
        print('uploading to google drive...')

        file = service.files().create(body=file_metadata,
                                      media_body=media,
                                      supportsAllDrives=True,
                                      fields='webViewLink').execute()
        print (f'uploaded to: {file.get("webViewLink")}')
        
    except HttpError as error:
        # TODO(developer) - Handle errors from drive API.
        print(f'An error occurred: {error}')
    
    os.remove(fname)

if __name__ == '__main__':
    main()
    