import polars as pl

def deas(p:str) -> pl.LazyFrame:
    '''
    returns a lazyframe from the full dea fixed width file
    p:
    for az prescribers: presc
    for az pharmacies: pharm
    for all az registrants: az
    for all registrants: all
    '''
    # widths and names according to the file format specifications provided by the DEA
    dea_widths = [9, 1, 16, 8, 40, 40, 40, 40, 33, 2, 5, 2, 1, 8, 10, 20, 20]
    dea_names = [
        'DEA Number', 'Business Activity Code', 'Drug Schedules', 'Expiration Date', 'Name',
        'Additional Company Info', 'Address 1', 'Address 2', 'City', 'State', 'Zip Code', 'Business Activity Sub Code',
        'Payment Indicator', 'Activity', 'Degree', 'State License Number', 'State CS License Number'
        ]
    slice_tuples = []
    offset = 0

    for w in dea_widths:
        slice_tuples.append((offset, w))
        offset += w

    # using unit separator '\x1F' to trick pyarrow into only making one col, unlikely to make it's way into this latin-1 file
    deas = pl.read_csv('data/cs_active.txt', encoding='latin-1', has_header=False, new_columns=['full_str'], use_pyarrow=True, separator='\x1F')

    deas = (
        deas
        .with_columns(
            [pl.col('full_str').str.slice(slice_tuple[0], slice_tuple[1]).str.strip().alias(col) for slice_tuple, col in zip(slice_tuples, dea_names)]
        )
        .drop('full_str')
    )

    if p == 'pharm':
        deas_pharm = (
            deas
            .filter(
                (pl.col('State') == 'AZ') &
                (pl.col('Business Activity Code') == 'A')
            )
        )
        print(deas_pharm.head())
        return deas_pharm.lazy()
    elif p == 'presc':
        deas_presc = (
            deas
            .filter(
                (pl.col('State') == 'AZ') &
                ((pl.col('Business Activity Code') == 'C') | (pl.col('Business Activity Code') == 'M'))
            )
        )
        print(deas_presc.head())
        return deas_presc.lazy()
    elif p == 'az':
        deas_az = (
            deas
            .filter(
                (pl.col('State') == 'AZ')
            )
        )
        print(deas_az.head())
        return deas_az.lazy()
    elif p == 'all':
        print(deas.head())
        return deas.lazy()
