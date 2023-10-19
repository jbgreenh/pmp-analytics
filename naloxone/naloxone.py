import polars as pl
import datetime
import toml
from utils.email import *

def naloxone_file():
    naloxone = pl.read_csv('data/naloxone_data.csv')
    total_naloxone = naloxone['Prescription Count'].sum()

    total_naloxone_str = '{:,}'.format(total_naloxone)

    today = datetime.datetime.now().strftime('%m%d%Y')
    tod = 'Morning' if datetime.datetime.now().hour < 12 else 'Afternoon'

    file_path = f'data/naloxone_{today}.xlsx'
    naloxone.write_excel(
        file_path, 
        worksheet='naloxone', 
        column_totals=['Prescription Count'], 
        autofit=True, 
        freeze_panes=((1,0,0,0)), 
        dtype_formats={pl.INTEGER_DTYPES:'0'}
    )
    print(f'naloxone data exported to {file_path}')
    print(f'total naloxone: {total_naloxone_str}')
    return file_path, total_naloxone_str, tod


def main():
    with open('../secrets.toml', 'r') as f:
        secrets = toml.load(f)

    sender = secrets['email']['email']
    to = secrets['email']['naloxone']
    signature = secrets['email']['sig'].replace(r'\n', '\n')
    subject = 'Weekly Naloxone Report'
    file_path, total_naloxone_str, tod = naloxone_file()
    
    message_txt = f'Good {tod} DHS Team-\n\nWe are now up to {total_naloxone_str} doses of naloxone dispensed.{signature}'
    message = create_message_with_attachment(sender=sender, to=to, subject=subject, message_text=message_txt, file_path=file_path)
    send_email(message=message)

if __name__ == '__main__':
    main()