import polars as pl
import toml
import os

from googleapiclient.discovery import build
from utils import auth, drive, email, deas

creds = auth.auth()
service = build('drive', 'v3', credentials=creds)
awarxe = (
    drive.awarxe(service=service)
    .with_columns(
        pl.col('dea number').str.strip_chars().str.to_uppercase()
    )
    .select('dea number')
)

print('processing deas...')
az_presc_deas = (
    deas.deas('presc')
    .with_columns(
        pl.col('DEA Number').str.strip_chars().str.to_uppercase()
    )
)

with open('../secrets.toml', 'r') as f:
    secrets = toml.load(f)

exclude_degs = drive.lazyframe_from_id_and_sheetname(service=service, file_id=secrets['files']['exclude_degs'], sheet_name='exclude_degs')
boards = drive.lazyframe_from_id_and_sheetname(service=service, file_id=secrets['files']['deg_board'], sheet_name='deg_board')

# pattern to drop ')' '(' and '.' from Name
pattern = r'[().]'

unreg_prescribers = (
    az_presc_deas.lazy().with_context(awarxe.select(pl.all().suffix('_awarxe')))
    .with_columns(
        pl.col('DEA Number').is_in(pl.col('dea number_awarxe')).map_dict({True:'YES', False:'NO'}).alias('awarxe'),
        pl.col('Name').str.replace_all(pattern=pattern, value='').str.split(' ').list.get(-1).alias('temp_deg')  # drop ')' '(' and '.' from Name
    )
    .filter(pl.col('awarxe').str.contains('NO'))
)
unreg_prescribers.collect().write_csv('temp.csv')   # write and remove to get rid of the first context 🤢

unreg_prescribers_w_boards = (
    pl.scan_csv('temp.csv', infer_schema_length=10000).with_context(exclude_degs.select(pl.all().suffix('_exclude')))
    .with_columns(
        pl.when((pl.col('temp_deg').is_in(pl.col('deg_exclude')).not_()) & (pl.col('temp_deg').str.len_chars() > 1))
        .then(pl.col('temp_deg'))
        .otherwise(None)
        .alias('temp_deg_2')
    )
    .with_columns(
        pl.when(pl.col('Degree').str.len_chars()==0)
        .then(pl.col('temp_deg_2'))
        .otherwise(pl.col('Degree'))
        .alias('final_deg')
    )
    .collect()
    .join(
        boards.collect(), how='left', left_on='final_deg', right_on='degree'
    )
    .select(
        'awarxe', 'DEA Number', 'Name', 'Additional Company Info', 'Address 1', 'Address 2', 'City', 'State', 'Zip Code', 'final_deg','State License Number', 'board'
    )
    .rename({'final_deg':'degree'})
    .with_columns(
        pl.when(pl.col('degree').str.len_chars()==0)
        .then(None)
        .otherwise(pl.col('degree'))
        .alias('degree')
    )
    .drop_nulls(subset='degree')
)
unmatched = unreg_prescribers_w_boards.filter(pl.col('board').is_null()).select('degree')
if unmatched.shape[0]>0:
    print('unmatched degrees, either add to exclude_degs or deg_board')
    print(unmatched)
os.remove('temp.csv')   # cleanup
board_counts = (
    unreg_prescribers_w_boards['board']
    .value_counts()
    .rename({'counts':'unregistered_prescribers'})
    .filter(
        pl.col('board').is_in(['Veterinary','Military','Homeopathic']).not_()
    )
    .sort(by='unregistered_prescribers', descending=True)
)

total_unreg = board_counts.sum().fill_null('total')

# unregistered totals
stats = pl.concat([board_counts, total_unreg])
print(stats)

board_contacts = drive.lazyframe_from_id_and_sheetname(service=service, file_id=secrets['files']['board_contacts'], sheet_name='registration')

board_list = ['Dental', 'Medical', 'Naturopathic', 'Nursing', 'Optometry', 'Osteopathic', 'Physician Assistant', 'Podiatry']
board_dict = {}

# board_df, board_name, board_email(s) for each board above
for b in board_list:
    board_df = unreg_prescribers_w_boards.filter(pl.col('board').str.contains(b))
    board_info = board_contacts.filter(pl.col('Board').str.contains(b)).collect()
    board_name = board_info.item(0,'Board Name')
    board_email = board_info.item(0,'Email')
    board_dict[b] = (board_df, board_name, board_email)

# TODO make and send emails
