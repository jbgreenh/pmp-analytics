import polars as pl
from utils import tableau

excluded_ndcs = pl.scan_csv('data/excluded_ndcs.csv', infer_schema_length=0) # infer_schema_length=0 forces all types to utf-8 and maintains the leading 0s in NDCs

luid = tableau.find_view_luid('opiate_antagonists', 'opiate antagonists')
antagonists = (
    tableau.lazyframe_from_view_id(luid, infer_schema_length=0)
    .join(excluded_ndcs, on='NDC', how='anti')
    .rename(
        {'Generic Name':'drug'}
    )
    .select('NDC', 'drug')
)

new_ndcs = antagonists.collect()

if new_ndcs.is_empty():
    print('no new ndcs found')
else:
    print('please input exclusion list in awarxe')
    print(new_ndcs)

    new_file = pl.concat([excluded_ndcs.collect(), new_ndcs])
    new_file.write_csv('data/excluded_ndcs.csv')
    print('data/excluded_ndcs.csv updated')
