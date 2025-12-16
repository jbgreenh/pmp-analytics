from pathlib import Path

import polars as pl
from az_pmp_utils import deas, drive, files, tableau


def pull_awarxe() -> pl.DataFrame:
    """
    pulls the most current awarxe registrants file

    returns:
        a dataframe with all active awarxe registants
    """
    return drive.awarxe().collect()


def tab_awarxe() -> pl.LazyFrame:
    """
    pulls active awarxe registrants from tableau. this file differs from the one `pull_awarxe()` returns because each row is one registrant with all of their dea numbers rather than each row being one dea number.

    returns:
        a lazyframe with the tableau version of active awarxe registrants
    """
    print('pulling awarxe from tableau...')
    luid = tableau.find_view_luid('active_approved', 'tab_awarxe')
    return tableau.lazyframe_from_view_id(luid, infer_schema=False)


def read_all_deas() -> pl.LazyFrame:
    """
    helper function to process the dea file

    returns:
        a lazyframe of all dea registrants
    """
    return deas.deas()


def bad_deas(awarxe: pl.DataFrame) -> None:
    """
    writes csv files with awarxe registrations that have incorrect dea numbers (those that fail a pattern match for the first and those that do not pass the checksum for the second)

    args:
        awarxe: a dataframe with active awarxe registrations
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
            pl.col('dea number').str.slice(2, 6).str.split('').cast(pl.List(pl.Int64)).alias('numbers'),
            pl.col('dea number').str.slice(8, 1).str.to_integer(strict=False).alias('check'),
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
    )
    checksum_fp = 'data/awarxe_cleanup/dea_checksum_fail.csv'
    checksum.write_csv(checksum_fp)
    print(f'wrote {checksum_fp}')


def suffix_not_res(awarxe: pl.DataFrame) -> None:
    """
    writes a csv with all active awarxe registrants not in the resident or fellow roles that still have a dea suffix

    args:
        awarxe: a dataframe with active awarxe registrations
    """
    bad_suffix = (
        awarxe
        .filter(
            ((pl.col('dea suffix').is_not_null()) | (pl.col('dea number').str.contains('-'))) &
            ((pl.col('role title').str.to_lowercase().str.contains('resident').not_()) & (pl.col('role title').str.to_lowercase().str.contains('fellow').not_()))
        )
    )
    bs_fn = 'data/awarxe_cleanup/suffix_not_res_not_fellow.csv'
    bad_suffix.write_csv(bs_fn)
    print(f'wrote {bs_fn}')


def inactive_deas(dea_list: pl.LazyFrame) -> None:
    """
    writes csvs with all inactive deas associated to active awarxe registrations (one file with some but not all deas inactive and one file with all deas inactive)

    args:
        dea_list: lazyframe of all dea registrants
    """
    dea_nums = dea_list.collect()['DEA Number'].to_list()
    inactive = (
        tab_awarxe()
        .drop_nulls('Associated DEA Number(s)')
        .filter(
            pl.col('User Role').str.to_lowercase().str.contains('resident').not_() &
            pl.col('User Role').str.to_lowercase().str.contains('fellow').not_()
        )
        .with_columns(
            pl.col('Associated DEA Number(s)')
            .str.strip_chars().str.replace_all(r'\s', '').str.to_uppercase().str.split(',').alias('deas_list')
        )
        .with_columns(
            pl.col('deas_list').list.filter(pl.element().is_in(dea_nums)).alias('active_deas'),
            pl.col('deas_list').list.filter(pl.element().is_in(dea_nums).not_()).alias('inactive_deas'),
        )
        .with_columns(
            (pl.col('active_deas').list.len() == 0).alias('all_inactive'),
            ((pl.col('active_deas').list.len() > 0) & (pl.col('inactive_deas').list.len() > 0)).alias('some_inactive'),
        )
        .with_columns(
            pl.col('active_deas', 'inactive_deas').cast(pl.List(pl.String)).list.join(' | '),
        )
        .drop('deas_list')
        .collect(engine='streaming')
    )

    some_inactive = (
        inactive
        .filter(
            pl.col('some_inactive')
        )
        .sort(pl.col('inactive_deas').str.len_chars(), descending=True)
        .select('User ID', 'Day of DOB', 'Email Address', 'User Full Name', 'User Role', 'User Role Category', 'active_deas', 'inactive_deas')
    )

    all_inactive = (
        inactive
        .filter(
            pl.col('all_inactive')
        )
        .sort(pl.col('inactive_deas').str.len_chars(), descending=True)
        .select('User ID', 'Associated DEA Number(s)', 'Day of DOB', 'Email Address', 'User Full Name', 'User Role', 'User Role Category')
    )

    si_fn = 'data/awarxe_cleanup/some_inactive_deas.csv'
    ai_fn = 'data/awarxe_cleanup/all_inactive_deas.csv'
    some_inactive.write_csv(si_fn)
    print(f'wrote {si_fn}')
    all_inactive.write_csv(ai_fn)
    print(f'wrote {ai_fn}')


def bad_npis(awarxe: pl.DataFrame) -> None:
    """
    writes csvs for awarxe registrations that have bad npi numbers (are not 10 digits for the 1st and do not pass the checksum for the 2nd)

    args:
        awarxe: a dataframe with active awarxe registrations
    """
    pattern = r'^\d{10}$'
    npi_pattern_match = (
        awarxe
        .with_columns(
            pl.col('npi number').str.replace_all(r'[‭|‬]', '')  # noqa: PLE2502
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


def multiple_roles(awarxe: pl.DataFrame) -> None:
    """
    writes a csv with awarxe registrations that have multiple roles

    args:
        awarxe: a dataframe with active awarxe registrations
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


def multiple_deas(awarxe: pl.DataFrame, dea_list: pl.LazyFrame) -> None:
    """
    writes a csv with az prescribers with multiple dea numbers and at least one of those dea numbers not registered in awarxe

    args:
        awarxe: a dataframe with active awarxe registrations
        dea_list: lazyframe of all dea registrants
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
            pl.col('SSN') != ''  # noqa: PLC1901
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


def closed_pharmacies_in_mp() -> pl.LazyFrame:
    """finds pharmacies from manage pharmacies that are not open in igov"""
    mp_path = Path('data/pharmacies.csv')
    files.warn_file_age(mp_path)

    lr_path = Path('data/List Request.csv')
    files.warn_file_age(lr_path)

    mp = (
        pl.scan_csv(mp_path)
        .with_columns(
            pl.col('DEA').str.strip_chars().str.to_uppercase()
        )
        .filter(
            pl.col('Reporting Requirements') != 'Exempt'
        )
        .select('DEA', 'Pharmacy License Number', 'Reporting Requirements')
    )

    igov = (
        pl.scan_csv(lr_path, infer_schema=False)
        .filter(
            pl.col('Type') == 'Pharmacy'
        )
        .with_columns(
            pl.col('License/Permit #').str.strip_chars().str.to_uppercase()
        )
        .rename(
            {'License/Permit #': 'Pharmacy License Number'}
        )
        .select(
            'Pharmacy License Number', 'Status', 'Business Name', 'Street Address', 'Apt/Suite #',
            'City', 'State', 'Zip', 'Email', 'Phone'
        )
    )

    closed = (
        mp
        .join(igov, on='Pharmacy License Number', how='left')
        .filter(
            pl.col('Status').str.starts_with('OPEN').not_()
        )
    )

    closedfname = 'data/awarxe_cleanup/multiple_deas.csv'
    closed.collect().write_csv(closedfname)
    print(f'wrote {closedfname}')


if __name__ == '__main__':
    Path('data/awarxe_cleanup').mkdir(parents=True, exist_ok=True)
    awarxe = pull_awarxe()
    dea_list = read_all_deas()
    bad_deas(awarxe)
    multiple_deas(awarxe, dea_list)
    suffix_not_res(awarxe)
    inactive_deas(dea_list)
    bad_npis(awarxe)
    multiple_roles(awarxe)
    closed_pharmacies_in_mp()
