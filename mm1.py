import polars as pl
from az_pmp_utils import drive


def mm1() -> None:
    """prepares the medical marijuana audit for `mm2.py` and prints instructions for transitioning between the two scripts"""
    awarxe = (
        drive.awarxe()
        .filter(
            pl.col('dea number').is_not_null()
        )
        .with_columns(
            pl.col('last name').str.to_uppercase().str.strip_chars()
        )
        .with_columns(
            (pl.col('last name').str.slice(-3) + pl.col('professional license number').str.slice(-4)).alias('awarxe_code')
        )
        .select('last name', 'professional license number', 'dea number', 'awarxe_code')
    )

    mm = (
        pl.scan_csv('data/mm_audit.csv', infer_schema_length=10000)
        .with_columns(
            pl.col('Physician Name').str.to_uppercase().str.strip_chars(),
            pl.col('License Number').fill_null('NONE')
        )
    )

    old = (
        pl.read_excel('data/old_mm.xlsx')
        .select('Physician Id', 'DEA Number')
        .lazy()
    )

    deg_for_trimming = [' DO', ' MD', ' PA', ' NP', ' ND']   # add degrees with a leading space to be trimmed as needed

    for deg in deg_for_trimming:
        mm = (
            mm
            .with_columns(
                pl.col('Physician Name').str.strip_suffix(deg)
            )
            .with_columns(
                pl.col('Physician Name').str.strip_suffix(',')
            )
        )

        awarxe = (
            awarxe
            .with_columns(
                pl.col('last name').str.strip_suffix(deg)
            )
            .with_columns(
                pl.col('last name').str.strip_suffix(',')
            )
        )

    mm_merged = (mm.join(old, on='Physician Id', how='left'))
    mm_old_match = (
        mm_merged
        .filter(pl.col('DEA Number').is_not_null())
    )
    mm_no_old_match = (
        mm_merged
        .filter(pl.col('DEA Number').is_null())
        .with_columns(
            (pl.col('Physician Name').str.slice(-3) + pl.col('License Number').str.slice(-4)).alias('mm_code')
        )
        .join(awarxe, left_on='mm_code', right_on='awarxe_code', how='left')
        .drop('mm_code', 'DEA Number')
        .rename({'dea number': 'DEA Number'})
    )
    mm_code_match = (
        mm_no_old_match
        .filter(pl.col('DEA Number').is_not_null())
        .drop('last name', 'professional license number')
    )
    mm_match_neither = (
        mm_no_old_match
        .filter(pl.col('DEA Number').is_null())
        .sort('Application Count', descending=True)
        .with_columns(
            pl.lit('').alias('note')
        )
        .drop('last name', 'professional license number')
    )

    mm_matches_combined = pl.concat([mm_old_match, mm_code_match])
    mm_matches_combined.collect().write_csv('data/mm_matches_combined.csv')
    mm_match_neither.collect().write_csv('data/mm_manual.csv')
    print('generated data/mm_matches_combined.csv')
    print('generated data/mm_manual.csv')
    print("""
        --------------------------------------------------
        please manually check all prescribers in mm_manual
        with 20+ application count for DEA numbers
        and save to data/mm_manual.csv, then run mm2.py
        --------------------------------------------------
    """)


if __name__ == '__main__':
    mm1()
