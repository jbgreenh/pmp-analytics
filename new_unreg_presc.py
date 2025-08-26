import os
import sys

# import io
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Literal, TypeAlias
from zoneinfo import ZoneInfo

import polars as pl
from dotenv import load_dotenv
from googleapiclient.discovery import build

from utils import auth, deas, drive  #, email

# from PyPDF2 import PdfReader, PdfWriter

UploadFileType: TypeAlias = Literal['sheet', 'csv', 'none']

def ft_default_factory() -> UploadFileType:
    return 'none'

@dataclass
class BoardInfo:
    """
    a class with the info needed to email the boards

    args:
        `board_name`: a `str` with the name of the board, eg: 'Arizona Osteopathic Board'
        `board_emails`: a `str` with a comma seperated list of the contact email(s) for the board
        `uploads_folder`: a `str` with the google drive folder id for the board's respective uploads folder
        `upload_skip_rows`: an `int` indicating the number of rows to skip when reading the upload file
        `upload_file_type`: an `UploadFileType` indicating of the upload file type (whoa)
        `upload_select_expr`: an expression for getting column names in order, with the correct dtypes, named properly, etc.
        `upload_filter_expr`: an expression for filtering the upload file
        `cleaned_license_expr`: an expression for cleaning license numbers, alias must be `cleaned_lino`: eg `(pl.lit('OPT-') + pl.col('State License Number').str.replace_all('[^0-9]', '').str.zfill(6)).alias('cleaned_lino')`
        `board_df`: a `pl.DataFrame` with all unregistered prescribers from the relevant board
    """
    board_name: str
    board_emails: str
    uploads_folder: str = field(default_factory=str)
    upload_skip_rows: int = field(default_factory=int)
    upload_file_type: UploadFileType = field(default_factory=ft_default_factory)
    upload_select_expr: pl.Expr = field(default_factory=pl.Expr)
    upload_filter_expr: pl.Expr = field(default_factory=pl.Expr)
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
    assert isinstance(contacts_file, str), 'BOARD_CONTACTS_FILE not found'
    board_contacts = drive.lazyframe_from_id_and_sheetname(service=service, file_id=contacts_file, sheet_name='registration', infer_schema_length=100).collect()
    boards = board_contacts['Board'].to_list()
    boards_dict = {}
    for board in boards:
        bn = board_contacts.filter(pl.col('Board') == board)['Board Name'].first()
        assert isinstance(bn, str), 'Board Name not found'
        be = board_contacts.filter(pl.col('Board') == board)['Email'].first()
        assert isinstance(be, str), 'Board Email not found'
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

    today = date.today()
    az_presc = (
        deas.deas('presc')
        .with_columns(pl.col(['Date of Original Registration', 'Expiration Date']).str.to_date('%Y%m%d', strict=False))
        .filter(pl.col('Expiration Date') > today)
    )
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

def infer_board(service, unreg_deas:pl.LazyFrame) -> pl.LazyFrame:
    """
    infer degrees and then board, prints the number of deas for which a board was unable to be inferred

    args:
        `service`: a google drive service
        `unreg_deas`: a lazyframe of unregistered prescribers returned by `check_deas_for_registration()`

    returns:
        a lazyframe of unregistered prescribers with inferred degrees and board names
    """
    ex_degs_file = os.environ.get('EXCLUDE_DEGS_FILE')
    assert isinstance(ex_degs_file, str), 'EXCLUDE_DEGS_FILE not found'
    deg_board_file = os.environ.get('DEG_BOARD_FILE')
    assert isinstance(deg_board_file, str), 'DEG_BOARD_FILE not found'

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
        unmatched_degs.write_csv('data/unmatched.csv')
        unmatched_degs.write_clipboard()
        print('data/unmatched.csv updated and copied to clipboard:')
        print(unmatched_degs)
        sys.exit('unmatched degrees, either add to exclude_degs or deg_board')

    still_no_board = all_degs.filter(pl.col('board').is_null()).collect().height
    print(f'no board could be found or inferred for {still_no_board} of {all_degs.collect().height} deas')
    return all_degs.filter(pl.col('board').is_not_null())

def update_board_info_with_uploaders(board_contacts:dict) -> dict:
    """
    add uploader information to the board contacts to allow for custom handling of boards that provide an upload

    args:
        `board_contacts`: a dict of board contacts returned by `get_board_contacts()`

    returns:
        a dict of board contacts with added infor for uploaders
    """
    opto_folder = os.environ.get('OPTOMETRY_UPLOADS_FOLDER')
    assert isinstance(opto_folder, str), 'OPTOMETRY_UPLOADS_FOLDER not found'
    opto_select = (
        pl.col('First Name').str.to_uppercase().alias('first_name'),
        pl.col('Last Name').str.to_uppercase().alias('last_name'),
        pl.col('Date of Birth').str.to_date('%Y-%m-%d', strict=False).alias('dob'),
        pl.col('License Number').str.to_uppercase().alias('license_number'),
        pl.col('Status').str.to_uppercase().alias('status'),
        pl.col('Email').alias('board_email'),
    )
    opto_filter = (
        (pl.col('first_name') != 'TOTALS') &
        (pl.col('status') == 'ACTIVE')
    )
    opto_clean = (pl.lit('OPT-') + pl.col('State License Number').str.replace_all('[^0-9]', '').str.zfill(6)).alias('cleaned_lino')
    board_contacts['Optometry'].uploads_folder = opto_folder
    board_contacts['Optometry'].upload_skip_rows = 3
    board_contacts['Optometry'].upload_file_type = 'sheet'
    board_contacts['Optometry'].upload_select_expr = opto_select
    board_contacts['Optometry'].upload_filter_expr = opto_filter
    board_contacts['Optometry'].cleaned_license_expr = opto_clean

    osteo_folder = os.environ.get('OSTEOPATHIC_UPLOADS_FOLDER')
    assert isinstance(osteo_folder, str), 'OSTEOPATHIC_UPLOADS_FOLDER not found'
    osteo_ft = 'csv'
    osteo_select = (
        pl.col('registrant_first_name').str.to_uppercase().alias('first_name'),
        pl.col('registrant_last_name').str.to_uppercase().alias('last_name'),
        pl.col('registrant_date_of_birth').str.to_date('%m/%d/%Y').alias('dob'),
        pl.col('registrant_license_number').str.to_uppercase().alias('license_number'),
        pl.col('license_status').str.to_uppercase().alias('status'),
    )
    osteo_filter = (pl.col('status') == 'ACTIVE')
    osteo_clean = (
        pl.when(pl.col('State License Number').str.to_uppercase().str.starts_with('R'))
        .then(pl.col('State License Number').str.to_uppercase())
        .otherwise(pl.col('State License Number').str.replace_all('[^0-9]', '').str.zfill(6)).alias('cleaned_lino')
    )
    #TODO: check if this works properly once we get the real upload with leading 0s (sample doesn't have them)

    board_contacts['Osteopathic'].uploads_folder = osteo_folder
    board_contacts['Osteopathic'].upload_file_type = osteo_ft
    board_contacts['Osteopathic'].upload_select_expr = osteo_select
    board_contacts['Osteopathic'].upload_filter_expr = osteo_filter
    board_contacts['Osteopathic'].cleaned_license_expr = osteo_clean

    return board_contacts

def add_dfs_to_board_info(service, unreg_presc:pl.LazyFrame, board_info:dict) -> dict:
    unreg_dir = Path('data/unreg_presc/')
    unreg_dir.mkdir(parents=True, exist_ok=True)
    for board, board_dict in board_info.items():
        print(f'processing for {board}...')
        if board_dict.upload_file_type == 'none':
            board_dict.board_df = unreg_presc.filter(pl.col('board') == board).drop('SSN', 'Tax ID').collect()
        else:
            latest_file = drive.get_latest_uploaded(service, folder_id=board_dict.uploads_folder, drive_ft=board_dict.upload_file_type, skip_rows=board_dict.upload_skip_rows, infer_schema=False)
            lf = latest_file.lf
            now = datetime.now(ZoneInfo('America/Phoenix'))
            age = now - latest_file.created_at
            age_hours = round(age.seconds/60/60,2) # don't need total_seconds() because of how we handle days below
            if age.days > 1:
                print(f'warning: {board} file is over a day old! using file created at {latest_file.created_at}, which was {age.days} days and {age_hours} hours ago')
            else:
                print(f'using {board} file created {age_hours} hours ago')

            unreg_presc_board = (
                unreg_presc
                .filter(pl.col('board') == board)
                .with_columns(board_dict.cleaned_license_expr)
            )

            upload_lf = (
                lf
                .select(board_dict.upload_select_expr)
                .filter(board_dict.upload_filter_expr)
            )

            upload_ez_match = (
                unreg_presc_board
                .join(upload_lf, how='inner', left_on='State License Number', right_on='license_number')
                .drop('first_name', 'last_name', 'dob')
            )

            upload_no_ez_match = (
                unreg_presc_board
                .join(upload_lf, how='anti', left_on='State License Number', right_on='license_number')
                .join(upload_lf, how='inner', left_on='cleaned_lino', right_on='license_number')
                .filter(pl.col('Name').str.contains(pl.col('first_name')))
                .with_columns(
                    pl.col('cleaned_lino').alias('State License Number')
                )
                .drop('first_name', 'last_name', 'dob')
            )

            upload_matches = pl.concat([upload_ez_match, upload_no_ez_match]).collect()
            upload_no_match = (
                unreg_presc_board
                .filter(
                    pl.col('DEA Number').is_in(upload_matches['DEA Number'].to_list()).not_()
                )
            ).collect()

            unmatch_path = unreg_dir / 'unmatched'
            unmatch_path.mkdir(parents=True, exist_ok=True)

            no_match_fp = unmatch_path / f'{board.lower()}_no_match.csv'
            upload_no_match.write_csv(no_match_fp)
            print(f'{no_match_fp} written')

            board_dict.board_df = upload_matches.drop('SSN', 'Tax ID') # , cleaned_lino | drop after testing
            #TODO: see above todo
        boards_dir = unreg_dir / 'boards'
        boards_dir.mkdir(parents=True, exist_ok=True)
        board_fp = boards_dir / f'{board.lower()}_unreg.csv'
        board_dict.board_df.write_csv(board_fp)
        print(f'{board_fp} written')
    return board_info


def main():
    load_dotenv()
    creds = auth.auth()
    service = build('drive', 'v3', credentials=creds)

    unreg_deas = check_deas_for_registration(service)

    unregistered_w_boards = infer_board(service, unreg_deas)

    board_contacts = get_board_contacts(service)

    board_info = update_board_info_with_uploaders(board_contacts)

    full_board_info = add_dfs_to_board_info(service, unregistered_w_boards, board_info)

if __name__ == '__main__':
    main()
