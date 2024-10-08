import polars as pl
from utils import deas

mp = (
    pl.scan_csv('data/pharmacies.csv', infer_schema_length=10000)
    .select('DEA')
).collect()['DEA'].to_list()

igov = (
    pl.scan_csv('data/List Request.csv', infer_schema_length=10000)
    .select('License/Permit #', 'Status', 'Email')
)

dea = (
    deas.deas('pharm')
    .filter(pl.col('DEA Number').is_in(mp).not_())
    .join(igov, how='left', left_on='State License Number', right_on='License/Permit #', coalesce=True)
    .select(
        'DEA Number', 'Name', 'State License Number','Additional Company Info', 'Address 1',
        'Address 2', 'City', 'State', 'Zip Code', 'Email', 'Status'
    )
)

dea.collect().write_csv('data/pharmacy_deas_not_in_mp.csv')
print('data/pharmacy_deas_not_in_mp.csv updated')
