import polars as pl
from utils import deas

mp = (
    pl.scan_csv('data/pharmacies.csv', infer_schema_length=10000)
    .select('DEA')
).collect()['DEA'].to_list()

igov = (
    pl.scan_csv('data/List Request.csv', infer_schema_length=0)
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
        pl.when(pl.col('Address 1').is_not_null())
            .then(pl.col('Address 1') + pl.lit(', ') + pl.col('Address 2'))
            .otherwise(pl.col('Address 2')).alias('Address')
    )
    .with_columns(
        pl.when(pl.col('Address 3').is_not_null())
            .then(pl.col('Address') + pl.lit(', ') + pl.col('Address 3'))
            .otherwise(pl.col('Address')).alias('Address')
    )

    .select(
        'DEA Number', 'Name', 'State License Number', 'Address',
        'City', 'State', 'Zip Code', 'Status', 'Email'
    )
)

dea.collect().write_csv('data/pharmacy_deas_not_in_mp.csv')
print('data/pharmacy_deas_not_in_mp.csv updated')
