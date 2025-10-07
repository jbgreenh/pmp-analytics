from pathlib import Path

import polars as pl

from utils import deas, files

# ruff: noqa: PLC1901
# polars cols with empty string are not falsey

mp_fp = Path('data/pharmacies.csv')
files.warn_file_age(mp_fp)
mp = (
    pl.scan_csv(mp_fp, infer_schema=False)
    .select('DEA')
).collect()['DEA'].to_list()

ig_fp = Path('data/List Request.csv')
files.warn_file_age(ig_fp)
igov = (
    pl.scan_csv(ig_fp, infer_schema=False)
    .filter(
        pl.col('Type') == 'Pharmacy'
    )
    .select('License/Permit #', 'Status', 'Email')
)

dea = (
    deas.deas('pharm')
    .filter(pl.col('DEA Number').is_in(mp).not_())
    .join(igov, how='left', left_on='State License Number', right_on='License/Permit #', coalesce=True)
    .with_columns(
        pl.when((pl.col('Address 1').is_not_null()) & (pl.col('Address 1') != ''))
            .then(pl.col('Address 1') + pl.lit(', ') + pl.col('Address 2'))
            .otherwise(pl.col('Address 2')).alias('Address')
    )
    .with_columns(
        pl.when((pl.col('Address 3').is_not_null()) & (pl.col('Address 3') != ''))
            .then(pl.col('Address') + pl.lit(', ') + pl.col('Address 3'))
            .otherwise(pl.col('Address')).alias('Address')
    )

    .select(
        'DEA Number', 'Name', 'State License Number', 'Address',
        'City', 'State', 'Zip Code', 'Status', 'Email'
    )
)

file_path = Path('data/pharmacy_deas_not_in_mp.csv')
dea.collect().write_csv(file_path)
print(f'{file_path} updated')
