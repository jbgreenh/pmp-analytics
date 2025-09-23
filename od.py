import argparse
from datetime import timedelta

import polars as pl
import polars_distance as pld

from utils import tableau


def process_ods(input_file: str, days_before: int, ratio: float) -> None:
    """
    process overdose data by checking dispensation data for matching date of birth and fuzzy matched on name. writes a disp and an odt file to the `data/od/` folder. the input file should be in the following format:

    | column     | description                        |
    |------------|------------------------------------|
    | LAST NAME  |                                    |
    | FIRST NAME |                                    |
    | DOD        | date of overdose MM/DD/YYYY format |
    | DOB        | date of birth MM/DD/YYYY format    |

    args:
        input_file: file name in the `data/od/` folder without the .csv extension
        days_before: the number of days before `DOD` to check for dispensations
        ratio: how similar names should be using a jaro-winkler ratio to consider a dispensation a match
    """
    ods = (
        pl.read_csv(f'data/od/{input_file}.csv')
        .with_columns(
            pl.col(['DOD', 'DOB']).str.to_date('%m/%d/%Y'),
            (pl.col('FIRST NAME') + ' ' + pl.col('LAST NAME')).str.to_uppercase().alias('full_name')
        )
        .drop('FIRST NAME', 'LAST NAME')
    )

    print('pulling luids...')
    od_luid = tableau.find_view_luid('od_disp', 'od')
    print(f'found luid: {od_luid}')
    odt_luid = tableau.find_view_luid('odt', 'od')
    print(f'found luid: {odt_luid}')
    disp_data = pl.DataFrame()
    odt_data = pl.DataFrame()
    for row in ods.iter_rows(named=True):
        start_date = row['DOD'] - timedelta(days=days_before)
        end_date = row['DOD'] + timedelta(days=1)
        dob = row['DOB']
        filters = {
            'start_date': start_date,
            'end_date': end_date,
            'search_dob': dob
        }
        print(f'pulling disp data for {dob}...')
        try:
            disp_df = tableau.lazyframe_from_view_id(od_luid, filters=filters, infer_schema=False)
        except tableau.TableauNoDataError:
            print(f'no disp data found for {start_date} - {end_date} with dob: {dob}')
        else:
            disp_data = pl.concat([disp_data, disp_df.collect()])
            print('disp data pulled')

        print(f'pulling odt data for {dob}...')
        try:
            odt_df = tableau.lazyframe_from_view_id(odt_luid, filters=filters)
        except tableau.TableauNoDataError:
            print(f'no odt data found for dob: {dob}')
        else:
            odt_data = pl.concat([odt_data, odt_df.collect()])
            print('odt data pulled')

    print(disp_data)
    print(odt_data)

    if disp_data.height > 1:
        disp_data = (
            disp_data
            .with_columns(
                pl.col(['Day of Filled At', 'Day of Patient Birthdate']).str.to_date('%B %d, %Y'),
                (pl.col('Orig Patient First Name') + ' ' + pl.col('Orig Patient Last Name')).str.to_uppercase().alias('orig_patient_name')
            )
            .drop('Orig Patient First Name', 'Orig Patient Last Name')
        )
        ods_disp = (
            ods
            .join(disp_data, how='left', left_on='DOB', right_on='Day of Patient Birthdate')
            .with_columns(
                (1 - pld.col('full_name').dist_str.jaro_winkler('orig_patient_name')).alias('ratio')
            )
            .filter(pl.col('ratio') >= ratio)
            .sort('DOD', 'Day of Filled At')
            .select(
                'DOB',
                'full_name',
                'orig_patient_name',
                'ratio',
                'Age Band (Age Groups)',
                'Patient Gender',
                'DOD',
                'Day of Filled At',
                'Generic Name',
                'Drug Schedule',
                'AHFS Description',
                'Dosage Unit',
                'Strength',
                'Days Supply',
                'Quantity',
                'Daily MME',
            )
        )

        print(ods_disp)
        disp_fn = f'data/od/{input_file}_disp.csv'
        ods_disp.write_csv(disp_fn)
        print(f'wrote {disp_fn}')

    if odt_data.height > 1:
        odt_data = (
           odt_data
            .with_columns(
                pl.col(['Day of Filled At', 'Day of Patient Birthdate']).str.to_date('%B %d, %Y'),
                (pl.col('Orig Patient First Name') + ' ' + pl.col('Orig Patient Last Name')).alias('orig_patient_name')
            )
            .drop('Orig Patient First Name', 'Orig Patient Last Name')
        )
        ods_odt = (
            ods
            .join(odt_data, how='left', left_on='DOB', right_on='Day of Patient Birthdate')
            .with_columns(
                (1 - pld.col('full_name').dist_str.jaro_winkler('orig_patient_name')).alias('ratio')
            )
            .filter(pl.col('ratio') >= ratio)
            .sort('DOD', 'Day of Filled At')
            .select(
                'DOB',
                'full_name',
                'orig_patient_name',
                'ratio',
                'DOD',
                'Day of Filled At',
                'Generic Name',
            )
        )
        print(ods_odt)
        odt_fn = f'data/od/{input_file}_odt.csv'
        ods_odt.write_csv(odt_fn)
        print(f'wrote {odt_fn}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='configure constants')

    parser.add_argument('-f', '--file', type=str, default='od', help='file name to be inspected; no extension (default: %(default)s)')
    parser.add_argument('-d', '--days-before', type=int, default=90, help='max number of days before DOD to consider a dispensation a match (default: %(default)s)')
    parser.add_argument('-r', '--ratio', type=float, default=0.8, help='patient name similarity ratio for dispensation to be considered a match (default: %(default)s)')

    args = parser.parse_args()

    process_ods(args.file, args.days_before, args.ratio)
