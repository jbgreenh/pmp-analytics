import polars as pl
import pandas as pd

techs = (
    pl.from_pandas(pd.read_html('data/techs/techs.xls', header=1)[0])
    .filter(
        pl.col('Status').str.to_lowercase().str.starts_with('open')
    )
    .with_columns(
        pl.col(['Expiration Date', 'Application Date', 'Issue Date']).str.to_date('%m/%d/%Y')
    )
)
superseded = (
    pl.from_pandas(pd.read_html('data/techs/superseded.xls', header=1)[0])
    .with_columns(
        pl.col(['Expiration Date', 'Application Date', 'Issue Date']).str.to_date('%m/%d/%Y')
    )
)

s_to_t = (
    techs.join(superseded, on='SSN', how='inner', suffix='_sup')
    .with_columns(
        pl.col('Issue Date').sub(pl.col('Issue Date_sup')).alias('time_delta')
    )
    .with_columns(
        pl.col('time_delta').dt.total_days().alias('days_to_tech')
    )
    .select(
        'License #', 'Type', 'Type_sup', 'Status', 'Status_sup',
        'First Name', 'Middle Name', 'Last Name', 'Issue Date',
        'Issue Date_sup', 'days_to_tech'
    )
)

print(s_to_t.select('days_to_tech').describe())
s_to_t.write_csv('data/techs/s_to_t.csv')
