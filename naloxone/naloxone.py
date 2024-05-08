import polars as pl
import datetime
import toml
from utils import auth, email, tableau
from googleapiclient.discovery import build

def naloxone_file():
    luid = tableau.find_view_luid('naloxone', 'Naloxone (2017-Present)')
    ys = []
    for y in range(2017, datetime.date.today().year+1):
        ys.append(str(y))
    years = ','.join(ys)
    filters = {'Year':years}
    naloxone = (
        tableau.lazyframe_from_view_id(luid, filters)
        .with_columns(
            pl.col('Prescription Count').str.replace_all(',','').cast(pl.Int32)
        )
        .collect()
    )
    total_naloxone = naloxone['Prescription Count'].sum()
    print(total_naloxone)

    total_naloxone_str = '{:,}'.format(total_naloxone)

    today = datetime.datetime.now().strftime('%m%d%Y')
    tod = 'Morning' if datetime.datetime.now().hour < 12 else 'Afternoon'

    file_paths = [f'data/naloxone_{today}.xlsx']
    naloxone.write_excel(
        workbook=file_paths[0], 
        worksheet='naloxone', 
        column_totals=['Prescription Count'], 
        autofit=True, 
        freeze_panes=((1,0,0,0)), 
        dtype_formats={pl.INTEGER_DTYPES:'0'}
    )
    print(f'naloxone data exported to {file_paths[0]}')
    print(f'total naloxone: {total_naloxone_str}')
    return file_paths, total_naloxone_str, tod


def main():
    with open('../secrets.toml', 'r') as f:
        secrets = toml.load(f)

    sender = secrets['email']['data']
    to = secrets['email']['naloxone']
    signature = secrets['email']['data_sig'].replace(r'\n', '\n')
    subject = 'Weekly Naloxone Report'
    file_paths, total_naloxone_str, tod = naloxone_file()
    
    message_txt = f'Good {tod} DHS Team-\n\nWe are now up to {total_naloxone_str} doses of naloxone dispensed.{signature}'
    message = email.create_message_with_attachments(sender=sender, to=to, subject=subject, message_text=message_txt, file_paths=file_paths)
    
    creds = auth.auth()
    service = build('gmail', 'v1', credentials=creds)
    email.send_email(service=service, message=message)

if __name__ == '__main__':
    main()