import polars as pl

excluded_ndcs = pl.scan_csv('data/excluded_ndcs.csv', infer_schema_length=0) # infer_schema_length=0 forces all types to utf-8 and maintains the leading 0s in NDCs
antagonists = (
    pl.scan_csv('data/opiate_antagonists_data.csv', infer_schema_length=0)
    .join(excluded_ndcs, on='NDC', how='anti')
    .rename(
        {'Generic Name':'drug'}
    )
)
new_ndcs = antagonists.collect()

if new_ndcs.shape[0] == 0:
    print('no new ndcs found')
else:
    print('please input exclusion list in awarxe')
    print(new_ndcs)

    # update the list
    new_file = pl.concat([excluded_ndcs.collect(), new_ndcs], how='vertical')
    new_file.write_csv('data/excluded_ndcs.csv')
    print('data/excluded_ndcs.csv updated')