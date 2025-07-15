from pathlib import Path

import polars as pl
from googleapiclient.discovery import build

from utils import auth, deas, drive, tableau


def pull_awarxe() -> pl.DataFrame:
    """
    pulls the mose current awarxe registrants file

    returns:
        a dataframe with all active awarxe registants
    """
    creds = auth.auth()
    service = build('drive', 'v3', credentials=creds)

    awarxe = drive.awarxe(service).collect()
    return awarxe

def tab_awarxe():
    """writes a file with tableau awarxe registrants; the tableau file has associated deas rather than an entry for each dea number"""
    print('pulling awarxe from tableau...')
    luid = tableau.find_view_luid('UsersEx','UsersEx')
    tab_awarxe = tableau.lazyframe_from_view_id(luid, infer_schema_length=10000)
    if tab_awarxe is not None:
        fn = 'data/tab_awarxe.csv'
        tab_awarxe.collect().write_csv(fn)
        print(f'wrote {fn}')
    else:
        print('no data in tableau awarxe')

def read_all_deas() -> pl.LazyFrame:
    """
    helper function to process the dea file

    returns:
        a lazyframe of all dea registrants
    """
    return deas.deas()

def bad_deas(awarxe:pl.DataFrame):
    """
    writes csv files with awarxe registrations that have incorrect dea numbers (those that fail a pattern match for the first and those that do not pass the checksum for the second)

    args:
        `awarxe`: a dataframe with active awarxe registrations
    """
    pattern = r'^[ABCFGHMPRabcfghmpr][A-Za-z](?:[0-9]{6}[1-9]|[0-9]{5}[1-9][0-9]|[0-9]{4}[1-9][0-9]{2}|[0-9]{3}[1-9][0-9]{3}|[0-9]{2}[1-9][0-9]{4}|[0-9][1-9][0-9]{5}|[1-9][0-9]{6})$'
    pattern_match = (
        awarxe
        .filter(pl.col('dea number').str.contains(pattern).not_() & pl.col('dea number').is_not_null())
        .sort(pl.col('dea number'))
        .select('email address', pl.col('dea number').str.to_uppercase(), 'dea suffix', 'first name', 'last name', 'role category', 'role title', 'registration review date')
    )
    pattern_fp = 'data/awarxe_cleanup/dea_pattern_match_fail.csv'
    pattern_match.write_csv(pattern_fp)
    print(f'wrote {pattern_fp}')

    checksum = (
        awarxe
        .filter(pl.col('dea number').str.contains(pattern))
        .with_columns(
            pl.col('dea number').str.slice(2,6).str.split('').cast(pl.List(pl.Int64)).alias('numbers'),
            pl.col('dea number').str.slice(8,1).str.to_integer(strict=False).alias('check'),
        )
        .with_columns(
            (
                (pl.col('numbers').list.gather_every(2, 0).list.sum() +
                (pl.col('numbers').list.gather_every(2, 1) * 2).list.sum()) % 10 == pl.col('check')
            ).alias('valid'),
        )
        .filter(
            pl.col('valid').not_()
        )
        .sort(pl.col('dea number'))
        .select('email address', pl.col('dea number').str.to_uppercase(), 'dea suffix', 'first name', 'last name', 'role category', 'role title', 'registration review date')
        # .select(pl.col('dea number'))
    )
    checksum_fp = 'data/awarxe_cleanup/dea_checksum_fail.csv'
    checksum.write_csv(checksum_fp)
    print(f'wrote {checksum_fp}')

def inactive_deas(awarxe:pl.DataFrame, dea_list:pl.LazyFrame):
    """
    writes a csv with all inactive deas associated to active awarxe registrations

    args:
        `awarxe`: a dataframe with active awarxe registrations
        `dea_list`: lazyframe of all dea registrants
    """
    inactive = (
        awarxe
        .drop_nulls('dea number')
        .with_columns(
            pl.col('dea number').str.strip_chars().str.to_uppercase()
        )
        .join(
            dea_list.select('DEA Number').collect(), left_on='dea number', right_on='DEA Number', how='anti'
        )
        .sort(pl.col('dea number'))
        .select('email address', pl.col('dea number').str.to_uppercase(), 'dea suffix', 'first name', 'last name', 'role category', 'role title', 'registration review date')
    )
    inactive_fp = 'data/awarxe_cleanup/inactive_deas.csv'
    inactive.write_csv(inactive_fp)
    print(f'wrote {inactive_fp}')

def bad_npis(awarxe:pl.DataFrame):
    """
    writes csvs for awarxe registrations that have bad npi numbers (are not 10 digits for the 1st and do not pass the checksum for the 2nd)

    args:
        `awarxe`: a dataframe with active awarxe registrations
    """
    pattern = r'^\d{10}$'
    npi_pattern_match = (
        awarxe
        .with_columns(
            pl.col('npi number').str.replace_all(r'[‭|‬]', '')
        )
        .filter(
            pl.col('npi number').str.contains(pattern).not_() & pl.col('npi number').is_not_null()
        )
        .sort('npi number')
        .select('email address', 'npi number', 'dea number', 'dea suffix', 'first name', 'last name', 'role category', 'role title', 'registration review date')
    )
    npi_pattern_match_fp = 'data/awarxe_cleanup/npi_pattern_match_fail.csv'
    npi_pattern_match.write_csv(npi_pattern_match_fp)
    print(f'wrote {npi_pattern_match_fp}')

    npi_checksum = (
        awarxe
        .filter(
            pl.col('npi number').str.contains(pattern)
        )
        .with_columns(
            pl.col('npi number').str.slice(0, 9).str.split('').cast(pl.List(pl.Int64)).alias('first_nine'),
            pl.col('npi number').str.slice(9, 1).str.to_integer().alias('check'),
        )
        .with_columns(
            (
                ((pl.col('first_nine').list.gather_every(2, 0) * 2).list.eval((pl.element() // 10) + (pl.element() % 10)).list.sum() +
                pl.col('first_nine').list.gather_every(2, 1).list.sum() +
                24 + pl.col('check')) % 10 == 0
            ).alias('valid'),
        )
        .filter(pl.col('valid').not_())
        .sort('npi number')
        .select('email address', 'npi number', 'dea number', 'dea suffix', 'first name', 'last name', 'role category', 'role title', 'registration review date')
    )
    checksum_fp = 'data/awarxe_cleanup/npi_checksum_fail.csv'
    npi_checksum.write_csv(checksum_fp)
    print(f'wrote {checksum_fp}')

def multiple_roles(awarxe:pl.DataFrame):
    """
    writes a csv with awarxe registrations that have multiple roles

    args:
        `awarxe`: a dataframe with active awarxe registrations
    """
    mult = (
        awarxe
        .with_columns(
            pl.col('role title').len().over('dea number').alias('role totals')
        )
        .filter(
            (pl.col('role totals') > 1) & (pl.col('dea suffix').is_null()) & (pl.col('dea number').is_not_null())
        )
    )
    mult_fp = 'data/awarxe_cleanup/multiple_roles.csv'
    mult.write_csv(mult_fp)
    print(f'wrote {mult_fp}')

def multiple_deas(awarxe:pl.DataFrame, dea_list:pl.LazyFrame):
    """
    writes a csv with az prescribers with multiple dea numbers and at least one of those dea numbers not registered in awarxe

    args:
        `awarxe`: a dataframe with active awarxe registrations
        `dea_list`: lazyframe of all dea registrants
    """
    awarxe_deas = awarxe['dea number'].to_list()

    prescribers = (
        dea_list
        .filter(
            (pl.col('State') == 'AZ') &
            ((pl.col('Business Activity Code') == 'C') | (pl.col('Business Activity Code') == 'M'))
        )
        .select(
            'Name',
            'DEA Number',
            'SSN',
            # 'Tax ID',
        )
    )

    names = (
        prescribers
        .filter(
            (pl.col('SSN') != '')
        )
        .with_columns(
            pl.col('Name').str.split(' ').list.get(1).alias('fname'),
        )
        .with_columns(
            (pl.col('SSN') + pl.col('fname')).alias('ssn_fname')
        )
        .group_by('ssn_fname')
        .agg('Name', 'DEA Number')
        .sort(pl.col('DEA Number').list.len(), descending=True)
        .filter(
            pl.col('DEA Number').list.len() > 1
        )
        .with_columns(
            pl.col('DEA Number').list.filter(pl.element().is_in(awarxe_deas)).alias('registered'),
            pl.col('DEA Number').list.filter(pl.element().is_in(awarxe_deas).not_()).alias('unregistered'),
            pl.col('Name').list.unique(),
        )
        .filter(
            pl.col('unregistered').list.len() > 0
        )
        .with_columns(
            pl.col('Name', 'registered', 'unregistered').cast(pl.List(pl.String)).list.join(' | '),
        )
        .drop('DEA Number')
    )
    mdfname = 'data/awarxe_cleanup/multiple_deas.csv'
    names.collect().write_csv(mdfname)
    print(f'wrote {mdfname}')


def main():
    Path('data/awarxe_cleanup').mkdir(parents=True, exist_ok=True)
    awarxe = pull_awarxe()
    # tab_awarxe()
    # tab_awarxe = pl.read_csv('data/tab_awarxe.csv', infer_schema=False)
    # TODO use tab_aware to check if all deas are inactive on an acct
    dea_list = read_all_deas()
    bad_deas(awarxe)
    multiple_deas(awarxe, dea_list)
    inactive_deas(awarxe, dea_list)
    bad_npis(awarxe)
    multiple_roles(awarxe)

if __name__ == '__main__':
    main()
