import sys
import polars as pl
import os
# import io
# from datetime import date, timedelta
from dataclasses import dataclass, field

from dotenv import load_dotenv
from googleapiclient.discovery import build
from utils import auth, deas, drive#, email
# from PyPDF2 import PdfReader, PdfWriter


@dataclass
class BoardInfo:
    """
    a class with the info needed to email the boards

    args:
        `board_name`: a `str` with the name of the board, eg: 'Arizona Osteopathic Board'
        `board_emails`: a `str` with a comma seperated list of the contact email(s) for the board
        `uploads_folder`: a `str` with the google drive folder id for the board's respective uploads folder
        `cleaned_license_expr`: an expression for cleaning license numbers: eg `(pl.lit('OPT-') + pl.col('State License Number').str.replace_all('[^0-9]', '').str.zfill(6)).alias('cleaned_lino')`
        `board_df`: a `pl.DataFrame` with all unregistered prescribers from the relevant board
    """
    board_name: str
    board_emails: str
    uploads_folder: str = field(default_factory=str)
    cleaned_license_expr: pl.Expr = field(default_factory=pl.Expr)
    board_df: pl.DataFrame = field(default_factory=pl.DataFrame)

def get_board_contacts(service) -> dict:
    """
    pulls board names and emails from the `BOARD_CONTACTS_FILE`

    args:
        `service`: a google drive service

    returns:
        a dictionary with a board name as the key and BoardInfo (with default uploads_folder, cleaned_license_expr, and board_df) as the value
    """
    contacts_file = os.environ.get('BOARD_CONTACTS_FILE')
    assert type(contacts_file) is str
    board_contacts = drive.lazyframe_from_id_and_sheetname(service=service, file_id=contacts_file, sheet_name='registration', infer_schema_length=100).collect()
    boards = board_contacts['Board'].to_list()
    boards_dict = {}
    for board in boards:
        bn = board_contacts.filter(pl.col('Board') == board)['Board Name'].first()
        assert(type(bn) is str)
        be = board_contacts.filter(pl.col('Board') == board)['Email'].first()
        assert(type(be) is str)
        boards_dict[board] = BoardInfo(board_name=bn, board_emails=be)
    return boards_dict

def check_deas_for_registration(service) -> pl.LazyFrame:
    """
    return a lazyframe with DEA registrations that are not also registered in awarxe

    args:
        `service`: a google drive service

    returns:
        a `LazyFrame` with unregistered DEAs
    """
    awarxe = (
        drive
        .awarxe(service)
        .collect()
        .drop_nulls('dea number')
        .filter(
            pl.col('dea suffix').is_null()
        )
        .with_columns(
            pl.col('dea number').str.strip_chars().str.to_uppercase()
        )
        ['dea number'].to_list()
    )

    az_presc = deas.deas('presc')
    unreg_presc = (
        az_presc
        .filter(
            pl.col('DEA Number').str.strip_chars().str.to_uppercase().is_in(awarxe).not_()
        )
        .filter(
            pl.col('Name').str.contains_any(['DVM', 'VMD']).not_() & pl.col('Degree').str.contains_any(['DVM', 'VMD']).not_()
        )
    )
    return unreg_presc

def infer_degrees(service, unreg_deas:pl.LazyFrame) -> pl.LazyFrame:
    """
    infer degrees

    args:
        `service` ([TODO:parameter]): [TODO:description]
        `unreg_deas`: [TODO:description]

    returns:
        [TODO:return]
    """
    ex_degs_file = os.environ.get('EXCLUDE_DEGS_FILE')
    assert type(ex_degs_file) is str
    deg_board_file = os.environ.get('DEG_BOARD_FILE')
    assert type(deg_board_file) is str

    exclude_degs = drive.lazyframe_from_id_and_sheetname(service=service, file_id=ex_degs_file, sheet_name='exclude_degs', infer_schema_length=100)
    deg_exclude = exclude_degs.collect()['deg'].to_list()
    boards = drive.lazyframe_from_id_and_sheetname(service=service, file_id=deg_board_file, sheet_name='deg_board', infer_schema_length=100)

    with_deg = unreg_deas.filter(pl.col('Degree').is_not_null() & (pl.col('Degree') != ''))
    without_deg = unreg_deas.filter(pl.col('Degree').is_null() | (pl.col('Degree') == ''))

    # pattern to drop ')' '(' and '.' from Name
    pattern = r'[().]'
    inferred_degs = (
        without_deg
        .with_columns(
            pl.col('Name').str.replace_all(pattern=pattern, value='').str.split(' ').list.get(-1).alias('temp_deg')  # drop ')' '(' and '.' from Name
        )
        .with_columns(
            pl.when((pl.col('temp_deg').is_in(deg_exclude).not_()) & (pl.col('temp_deg').str.len_chars() > 1))
                .then(pl.col('temp_deg'))
                .otherwise(None).alias('Degree')
        )
        .drop('temp_deg')
    )
    all_degs = pl.concat([with_deg, inferred_degs]).join(boards, how='left', left_on='Degree', right_on='degree', coalesce=True)
    unmatched = all_degs.filter(pl.col('Degree').is_not_null() & pl.col('board').is_null()).collect()
    if not unmatched.is_empty():
        unmatched_degs = unmatched['Degree'].value_counts(sort=True)
        print('data/unmatched.csv updated:')
        print(unmatched_degs)   # cleanup
        unmatched_degs.write_csv('data/unmatched.csv')
        sys.exit('unmatched degrees, either add to exclude_degs or deg_board')

    still_no_board = all_degs.filter(pl.col('board').is_null()).collect().height
    print(f'no board could be found or inferred for {still_no_board} deas')
    return all_degs.filter(pl.col('board').is_not_null())

def complete_board_info(service, unregistered_w_boards:pl.LazyFrame, board_contacts:dict):
    opto_folder = os.environ.get('OPTOMETRY_UPLOADS_FOLDER')
    assert type(opto_folder) is str
    opto_clean = (pl.lit('OPT-') + pl.col('State License Number').str.replace_all('[^0-9]', '').str.zfill(6)).alias('cleaned_lino')
    board_contacts['Optometry'].uploads_folder = opto_folder
    board_contacts['Optometry'].cleaned_license_expr = opto_clean

    osteo_folder = os.environ.get('OSTEOPATHIC_UPLOADS_FOLDER')
    assert type(osteo_folder) is str
    osteo_clean = (
        pl.when(pl.col('State License Number').str.to_uppercase().str.starts_with('R'))
        .then(pl.col('State License Number').str.to_uppercase().alias('cleaned_lino'))
        .otherwise(pl.col('State License Number').str.zfill(6).alias('cleaned_lino'))
    )
    board_contacts['Osteopathic'].uploads_folder = osteo_folder
    board_contacts['Osteopathic'].cleaned_license_expr = osteo_clean
    for key, value in board_contacts.items():
        print(f'{key = }')
        print(value.uploads_folder)
        #TODO: add df, handle for those with custom expressions


def main():
    load_dotenv()
    creds = auth.auth()
    service = build('drive', 'v3', credentials=creds)

    unreg_deas = check_deas_for_registration(service)

    unregistered_w_boards = infer_degrees(service, unreg_deas)

    board_contacts = get_board_contacts(service)
    complete_board_info(service, unregistered_w_boards, board_contacts)

if __name__ == '__main__':
    main()
