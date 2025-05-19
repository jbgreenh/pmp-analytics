from pathlib import Path

import polars as pl
from googleapiclient.discovery import build

from utils import auth, deas, drive, tableau


def pull_awarxe():
    creds = auth.auth()
    service = build('drive', 'v3', credentials=creds)

    print('pullng awarxe...')
    drive.awarxe(service).collect().write_csv('data/awarxe.csv')
    print('wrote data/awarxe.csv')

def tab_awarxe():
    print('pulling awarxe from tableau...')
    luid = tableau.find_view_luid('UsersEx','UsersEx')
    tableau.lazyframe_from_view_id(luid, infer_schema_length=10000).collect().write_csv('data/tab_awarxe.csv')
    print('wrote data/tab_awarxe.csv')

def read_all_deas():
    return deas.deas('all')

def bad_deas(awarxe):
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

def inactive_deas(awarxe, dea_list):
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

def bad_npis(awarxe):
    pattern = r'^\d{10}$'
    npi_pattern_match = (
        awarxe
        # .with_columns(
        #     pl.col('npi number').str.replace_all(r'[‭|‬]', '')
        # )
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


def main():
    Path('data/awarxe_cleanup').mkdir( parents=True, exist_ok=True)
    # pull_awarxe()
    # tab_awarxe()
    awarxe = pl.read_csv('data/awarxe.csv', infer_schema=False)
    # tab_awarxe = pl.read_csv('data/tab_awarxe.csv', infer_schema=False)
    # TODO use tab_aware to check if all deas are inactive on an acct
    dea_list = read_all_deas()
    bad_deas(awarxe)
    inactive_deas(awarxe, dea_list)
    bad_npis(awarxe)

if __name__ == '__main__':
    main()
