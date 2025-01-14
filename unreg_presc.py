import polars as pl
import toml
import os
import io
import sys
from datetime import date
from dataclasses import dataclass

from googleapiclient.discovery import build
from utils import auth, drive, email, deas
from PyPDF2 import PdfReader, PdfWriter

@dataclass
class BoardInfo:
    """
    a class with the info needed to email the boards

    args:
        `board_df`: a `pl.DataFrame` with all unregistered prescribers from the relevant board
        `board_name`: a `str` with the name of the board, eg: 'Optometry'
        `board_emails`: a `str` with a comma seperated list of the contact email(s) for the board
    """
    board_df: pl.DataFrame
    board_name: str
    board_emails: str

def get_board_dict(service) -> dict[str, BoardInfo]:
    """
    checks the dea list for prescriber registration in awarxe

    args:
        `service`: a google drive service

    returns:
        `board_dict`: a dictionary with the board name as the key and
        board_df:pl.Dataframe, board_name:str, board_email(s):str as values
    """
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

    with open('secrets.toml', 'r') as f:
        secrets = toml.load(f)

    exclude_degs = drive.lazyframe_from_id_and_sheetname(service=service, file_id=secrets['files']['exclude_degs'], sheet_name='exclude_degs', infer_schema_length=100)
    boards = drive.lazyframe_from_id_and_sheetname(service=service, file_id=secrets['files']['deg_board'], sheet_name='deg_board', infer_schema_length=100)

    # pattern to drop ')' '(' and '.' from Name
    pattern = r'[().]'

    awarxe_deas = awarxe.collect()['dea number']

    unreg_prescribers = (
        az_presc_deas
        .with_columns(
            pl.col('DEA Number').is_in(awarxe_deas).replace_strict({True:'YES', False:'NO'}).alias('awarxe'),
            pl.col('Name').str.replace_all(pattern=pattern, value='').str.split(' ').list.get(-1).alias('temp_deg')  # drop ')' '(' and '.' from Name
        )
        .filter(pl.col('awarxe').str.contains('NO'))
    )

    deg_exclude = exclude_degs.collect()['deg']

    unreg_prescribers_w_boards = (
        unreg_prescribers
        .with_columns(
            pl.when((pl.col('temp_deg').is_in(deg_exclude).not_()) & (pl.col('temp_deg').str.len_chars() > 1))
                .then(pl.col('temp_deg'))
                .otherwise(None).alias('temp_deg_2')
        )
        .with_columns(
            pl.when(pl.col('Degree').str.len_chars()==0)
                .then(pl.col('temp_deg_2'))
                .otherwise(pl.col('Degree')).alias('final_deg')
        )
        .collect()
        .join(
            boards.collect(), how='left', left_on='final_deg', right_on='degree', coalesce=True
        )
        .select(
            'awarxe', 'DEA Number', 'Name', 'Additional Company Info', 'Address 1', 'Address 2', 'City', 'State', 'Zip Code', 'final_deg','State License Number', 'board'
        )
        .rename({'final_deg':'degree'})
        .with_columns(
            pl.when(pl.col('degree').str.len_chars()==0)
                .then(None)
                .otherwise(pl.col('degree')).alias('degree')
        )
        .drop_nulls(subset='degree')
    )
    unmatched = unreg_prescribers_w_boards.filter(pl.col('board').is_null()).select('degree')
    if not unmatched.is_empty():
        print(unmatched)   # cleanup
        unmatched.write_csv('data/unmatched.csv')
        print('data/unmatched.csv updated')
        sys.exit('unmatched degrees, either add to exclude_degs or deg_board')

    # opto_folder = secrets['folders']['optometry_uploads']
    #
    # yesterday = date.today() - timedelta(days=1)
    # yesterday = date(year=2024, month=10, day=28) # for setting date manually, comment out if receiving the file daily
    # yesterday_str = yesterday.strftime('%Y%m%d')
    #
    # opto = (
    #     drive.lazyframe_from_file_name_sheet(service, file_name=f'Optometry Pharmacy Report_{yesterday_str}', folder_id=opto_folder, skip_rows=3)
    #     .filter(
    #         pl.col('First Name').str.to_lowercase().str.contains('totals').not_()
    #     )
    #     .collect()
    # )
    #
    # unreg_opto = (
    #     unreg_prescribers_w_boards.filter(pl.col('board') == "Optometry")
    # )
    #
    # unreg_opto_ez = (
    #     unreg_opto
    #     .join(opto, how='inner', left_on='State License Number', right_on='License Number')
    #     .drop('First Name', 'Last Name', 'Date of Birth')
    # )
    #
    # unreg_opto_no_ez = (
    #     unreg_opto
    #     .join(opto, how='anti', left_on='State License Number', right_on='License Number')
    #     .with_columns(
    #         (pl.lit('OPT-') + pl.col('State License Number').str.replace_all('[^0-9]', '').str.zfill(6)).alias('cleaned_lino')
    #     )
    # )
    #
    # unreg_no_ez_cleaned = (
    #     unreg_opto_no_ez
    #     .join(opto, how='inner', left_on='cleaned_lino', right_on='License Number')
    # )
    #
    # unreg_opto_cleaned_matches_good_names = (
    #     unreg_no_ez_cleaned
    #     .filter(
    #         pl.col('Name').str.contains(pl.col('First Name').str.to_uppercase())
    #     )
    #     .with_columns(
    #         pl.col('cleaned_lino').alias('State License Number')
    #     )
    #     .drop('cleaned_lino', 'First Name', 'Last Name', 'Date of Birth')
    # )
    #
    # opto_matches = pl.concat([unreg_opto_ez, unreg_opto_cleaned_matches_good_names]).sort(by='Status')
    #
    # # opto_no_match = (
    # #     unreg_opto
    # #     .filter(
    # #         pl.col('DEA Number').is_in(opto_matches['DEA Number']).not_()
    # #     )
    # # )
    # # opto_matches.write_csv('data/opto/deas/opto_matches.csv')
    # # opto_no_match.write_csv('data/opto/deas/opto_no_match.csv')

    unreg_prescribers_w_boards = (
        unreg_prescribers_w_boards
        # .filter(pl.col('board') != 'Optometry')
    )

    # unreg_prescribers_w_boards = pl.concat([unreg_prescribers_w_boards, opto_matches], how='diagonal')

    board_counts = (
        unreg_prescribers_w_boards['board']
        .value_counts()
        .rename({'count':'unregistered_prescribers'})
        .filter(
            pl.col('board').is_in(['Veterinary','Military','Homeopathic']).not_()
        )
        .sort(by='unregistered_prescribers', descending=True)
    )

    total_unreg = board_counts.sum().fill_null('total')

    # unregistered totals
    stats = pl.concat([board_counts, total_unreg])
    print(stats)

    board_contacts = drive.lazyframe_from_id_and_sheetname(service=service, file_id=secrets['files']['board_contacts'], sheet_name='registration', infer_schema_length=100)

    board_list = ['Dental', 'Medical', 'Naturopathic', 'Nursing', 'Optometry', 'Osteopathic', 'Physician Assistant', 'Podiatry']
    board_dict = {}

    # board_df, board_name, board_email(s) for each board above
    for b in board_list:
        board_df = unreg_prescribers_w_boards.filter(pl.col('board').str.contains(b))
        board_info = board_contacts.filter(pl.col('Board').str.contains(b)).collect()
        board_name = board_info.item(0,'Board Name')
        board_email = board_info.item(0,'Email')
        board_dict[b] = BoardInfo(board_df, board_name, board_email)

    return board_dict


def send_emails(board_dict:dict[str, BoardInfo], creds, service):
    """
    sends emails to each board with their unregistered prescribers

    args:
        `board_dict`: the `board_dict` returned by `get_board_dict()`
        `creds`: google api credentials
        `service`: a google drive service
    """
    with open('../secrets.toml', 'r') as f:
        secrets = toml.load(f)

    print('pulling RegistrationRequirementsNotice...')
    reg_req_notice = secrets['files']['unreg_prescribers']
    docs_service = build('docs', 'v1', credentials=creds)
    drive_service = service

    today = date.today()
    today_str = today.strftime('%B %d, %Y')

    copy_response = drive_service.files().copy(fileId=reg_req_notice, body={'name': 'copy'}, supportsAllDrives=True).execute()
    copy_doc_id = copy_response['id']

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

    export_response = drive_service.files().export(fileId=copy_doc_id, mimeType='application/pdf').execute()

    pdf_reader = PdfReader(io.BytesIO(export_response))
    if len(pdf_reader.pages) > 1:
        pdf_writer = PdfWriter()
        second_page = pdf_reader.pages[1]
        pdf_writer.add_page(second_page)
        with open('data/RegistrationRequirementsNotice.pdf', 'wb') as rrn:
            pdf_writer.write(rrn)
    else:
        with open('data/RegistrationRequirementsNotice.pdf', 'wb') as f:
            f.write(export_response)


    drive_service.files().delete(fileId=copy_doc_id, supportsAllDrives=True).execute()
    print('data/RegistrationRequirementsNotice.pdf updated')

    print('pulling unregistered prescriber folder...')
    reg_flyer = secrets['files']['unreg_presc_flyer']
    flyer_export = drive_service.files().export(fileId=reg_flyer, mimeType='application/pdf').execute()

    pdf_reader = PdfReader(io.BytesIO(flyer_export))
    if len(pdf_reader.pages) > 1:
        pdf_writer = PdfWriter()
        second_page = pdf_reader.pages[1]
        pdf_writer.add_page(second_page)
        with open('data/UnregisteredPrescriberFlyer.pdf', 'wb') as flyer:
            pdf_writer.write(flyer)
    else:
        with open('data/UnregisteredPrescriberFlyer.pdf', 'wb') as f:
            f.write(flyer_export)

    print('data/UnregisteredPrescriberFlyer.pdf updated')

    email_service = build('gmail', 'v1', credentials=creds)
    sender = secrets['email']['compliance']
    signature = secrets['email']['comp_sig'].replace(r'\n', '\n')

    # board_dict['board'] = (board_df, board_name, board_email)
    for board, info in board_dict.items():
        subj = f'CSPMP Unregistered Prescribers {info.board_name}'
        body = f'The CSPMP sends biannual compliance reports to Arizona regulatory licensing boards regarding prescribers who have been identified as non-compliant in registering for the CSPMP, pursuant to A.R.S § 36-2606 (A). This list is generated every six months.\n\nAttached you will find the list of licensed providers with the {info.board_name} that are not registered with the Arizona CSPMP, as well as detailed information pertaining to the registration requirements.\n\nIf you have any questions please feel free to contact us.{signature}'

        report_file = f'{board}_unregistered_prescribers_{today_str}.csv'
        info.board_df.write_csv(report_file)

        message = email.create_message_with_attachments(sender=sender, to=info.board_emails, subject=subj, message_text=body, file_paths=[report_file, 'data/RegistrationRequirementsNotice.pdf', 'data/UnregisteredPrescriberFlyer.pdf'], bcc=[sender])
        email.send_email(service=email_service, message=message)
        os.remove(report_file)



if __name__ == '__main__':
    creds = auth.auth()
    service = build('drive', 'v3', credentials=creds)
    board_dict = get_board_dict(service=service)

    # delete this loop and uncomment the email send when ready
    for board, info in board_dict.items():
        print(info.board_name)
        print(info.board_emails)
        info.board_df.write_csv(f'data/{board}.csv')

    # send_emails(board_dict=board_dict, creds=creds, service=service)
