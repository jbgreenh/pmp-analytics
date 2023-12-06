import polars as pl
import toml
import os
from datetime import date

from googleapiclient.discovery import build
from utils import auth, drive, email, deas

def get_board_dict(service):
    awarxe = (
        drive.awarxe(service=service)
        .with_columns(
            pl.col('dea number').str.strip_chars().str.to_uppercase()
        )
        .select('dea number')
    )

    print('processing deas...')
    az_presc_deas = (
        deas.deas('presc')
        .with_columns(
            pl.col('DEA Number').str.strip_chars().str.to_uppercase()
        )
    )

    with open('../secrets.toml', 'r') as f:
        secrets = toml.load(f)

    exclude_degs = drive.lazyframe_from_id_and_sheetname(service=service, file_id=secrets['files']['exclude_degs'], sheet_name='exclude_degs')
    boards = drive.lazyframe_from_id_and_sheetname(service=service, file_id=secrets['files']['deg_board'], sheet_name='deg_board')

    # pattern to drop ')' '(' and '.' from Name
    pattern = r'[().]'

    unreg_prescribers = (
        az_presc_deas.lazy().with_context(awarxe.select(pl.all().suffix('_awarxe')))
        .with_columns(
            pl.col('DEA Number').is_in(pl.col('dea number_awarxe')).map_dict({True:'YES', False:'NO'}).alias('awarxe'),
            pl.col('Name').str.replace_all(pattern=pattern, value='').str.split(' ').list.get(-1).alias('temp_deg')  # drop ')' '(' and '.' from Name
        )
        .filter(pl.col('awarxe').str.contains('NO'))
    )
    unreg_prescribers.collect().write_csv('temp.csv')   # write and remove to get rid of the first context 🤢

    unreg_prescribers_w_boards = (
        pl.scan_csv('temp.csv', infer_schema_length=10000).with_context(exclude_degs.select(pl.all().suffix('_exclude')))
        .with_columns(
            pl.when((pl.col('temp_deg').is_in(pl.col('deg_exclude')).not_()) & (pl.col('temp_deg').str.len_chars() > 1))
            .then(pl.col('temp_deg'))
            .otherwise(None)
            .alias('temp_deg_2')
        )
        .with_columns(
            pl.when(pl.col('Degree').str.len_chars()==0)
            .then(pl.col('temp_deg_2'))
            .otherwise(pl.col('Degree'))
            .alias('final_deg')
        )
        .collect()
        .join(
            boards.collect(), how='left', left_on='final_deg', right_on='degree'
        )
        .select(
            'awarxe', 'DEA Number', 'Name', 'Additional Company Info', 'Address 1', 'Address 2', 'City', 'State', 'Zip Code', 'final_deg','State License Number', 'board'
        )
        .rename({'final_deg':'degree'})
        .with_columns(
            pl.when(pl.col('degree').str.len_chars()==0)
            .then(None)
            .otherwise(pl.col('degree'))
            .alias('degree')
        )
        .drop_nulls(subset='degree')
    )
    unmatched = unreg_prescribers_w_boards.filter(pl.col('board').is_null()).select('degree')
    if unmatched.shape[0]>0:
        print('unmatched degrees, either add to exclude_degs or deg_board')
        print(unmatched)   # cleanup
        unmatched.write_csv('data/unmatched.csv')
        print('data/unmatched.csv updated')
        # os.remove('temp.csv')    TODO uncomment
        # return    TODO uncomment

    os.remove('temp.csv')   # cleanup
    board_counts = (
        unreg_prescribers_w_boards['board']
        .value_counts()
        .rename({'counts':'unregistered_prescribers'})
        .filter(
            pl.col('board').is_in(['Veterinary','Military','Homeopathic']).not_()
        )
        .sort(by='unregistered_prescribers', descending=True)
    )

    total_unreg = board_counts.sum().fill_null('total')

    # unregistered totals
    stats = pl.concat([board_counts, total_unreg])
    print(stats)

    board_contacts = drive.lazyframe_from_id_and_sheetname(service=service, file_id=secrets['files']['board_contacts'], sheet_name='testing')   # TODO change sheet_name to 'registration'

    board_list = ['Dental', 'Medical', 'Naturopathic', 'Nursing', 'Optometry', 'Osteopathic', 'Physician Assistant', 'Podiatry']
    board_dict = {}

    # board_df, board_name, board_email(s) for each board above
    for b in board_list:
        board_df = unreg_prescribers_w_boards.filter(pl.col('board').str.contains(b))
        board_info = board_contacts.filter(pl.col('Board').str.contains(b)).collect()
        board_name = board_info.item(0,'Board Name')
        board_email = board_info.item(0,'Email')
        board_dict[b] = (board_df, board_name, board_email)
    
    return board_dict


def send_emails(board_dict, creds, service):
    with open('../secrets.toml', 'r') as f:
        secrets = toml.load(f)

    print('pulling RegistrationRequirementsNotice...')
    reg_req_notice = secrets['files']['unreg_prescribers']
    docs_service = build('docs', 'v1', credentials=creds)
    drive_service = service

    today = date.today()
    today_str = today.strftime('%B %d, %Y')

    # copy the doc
    copy_response = drive_service.files().copy(fileId=reg_req_notice, body={'name': 'copy'}, supportsAllDrives=True).execute()
    copy_doc_id = copy_response['id']

    # replace text
    requests = [
        {
            'replaceAllText': {
                'containsText': {
                    'text': '{{date}}',
                    'matchCase': True
                },
                'replaceText': f'{today_str}'
            }
        }
    ]

    docs_service.documents().batchUpdate(documentId=copy_doc_id, body={'requests': requests}).execute()

    # export as pdf with support for shared drives
    export_response = drive_service.files().export(fileId=copy_doc_id, mimeType='application/pdf').execute()

    with open('data/RegistrationRequirementsNotice.pdf', 'wb') as f:
        f.write(export_response)

    # delete the copied doc from drive
    drive_service.files().delete(fileId=copy_doc_id, supportsAllDrives=True).execute()
    print('data/RegistrationRequirementsNotice.pdf updated')

    email_service = build('gmail', 'v1', credentials=creds)
    sender = secrets['email']['compliance']
    signature = secrets['email']['comp_sig'].replace(r'\n', '\n')
    
    # board_dict['board'] = (board_df, board_name, board_email)
    for board, stuff in board_dict.items():
        board_df = stuff[0]
        board_name = stuff[1]
        board_email = stuff[2]
        subj = f'CSPMP Unregistered Prescribers {board_name}'
        body = f'The CSPMP sends biannual compliance reports to Arizona regulatory licensing boards regarding prescribers who have been identified as non-compliant in registering for the CSPMP, pursuant to A.R.S § 36-2606 (A). This list is generated every six months.\n\nAttached you will find the list of licensed providers with the {board_name} that are not registered with the Arizona CSPMP, as well as detailed information pertaining to the registration requirements.\n\nIf you have any questions please feel free to contact us.{signature}'

        report_file = f'{board}_unregistered_prescribers_{today_str}.csv'
        board_df.write_csv(report_file)

        message = email.create_message_with_attachments(sender=sender, to=board_email, subject=subj, message_text=body, file_paths=[report_file, 'data/RegistrationRequirementsNotice.pdf'], bcc=[sender])
        email.send_email(service=email_service, message=message)
        os.remove(report_file)



if __name__ == '__main__':
    creds = auth.auth()
    service = build('drive', 'v3', credentials=creds)
    board_dict = get_board_dict(service=service)
    if board_dict:
        send_emails(board_dict=board_dict, creds=creds, service=service)
