import io
import os
from datetime import date, timedelta
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

import paramiko
import polars as pl
from az_pmp_utils import drive, tableau
from az_pmp_utils.tableau import TableauNoDataError
from dotenv import load_dotenv

from constants import MAX_SERVU_FILE_COUNT


def remove_oldest_file(sftp: paramiko.SFTPClient, remote_path: str) -> None:
    """
    removes the oldest file from the folder at `remote_path` in the sftp; maintains the `MAX_SERVU_FILE_COUNT` on the server

    args:
        sftp: paramiko SFTPClient
        remote_path: the path to remove the oldest file from
    """
    files = sftp.listdir_attr(path=remote_path)
    if len(files) > MAX_SERVU_FILE_COUNT:
        oldest_file = min(files, key=lambda f: f.st_mtime)  # type: ignore[reportArgumentType] | these files will have st_mtime
        print(f'removing oldest file: {oldest_file.filename}...')
        sftp.remove(oldest_file.filename)
        print('file removed')
    else:
        print(f'{len(files)} file(s) on servu, none removed')


if __name__ == '__main__':
    load_dotenv()

    sftp_host = os.environ['SERVU_HOST']
    sftp_port = os.environ['SERVU_PORT']
    sftp_user = os.environ['SERVU_USERNAME']
    sftp_password = os.environ['SERVU_PASSWORD']

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=sftp_host, port=int(sftp_port), username=sftp_user, password=sftp_password)
    sftp = ssh.open_sftp()

    try:
        last_forty = (
            drive.lazyframe_from_id_and_sheetname(os.environ['MU_REPORTS_FILE'], 'Data')
            .tail(40)
            .select(
                'board',
                'MM/YYYY',
                'prescriber_name',
                pl.col('dea_number(s)').str.split(', ')
            )
            .collect()
        )

        for board in last_forty['board'].value_counts()['board'].to_list():
            all_files = []
            luid = tableau.find_view_luid('with filters (auto)', 'mu recheck')
            for row in last_forty.iter_rows(named=True):
                presc_df = pl.DataFrame()
                for dea in row['dea_number(s)']:
                    mo, yr = row['MM/YYYY'].split('/')
                    filters = {
                        'prescriber_dea': dea,
                        'written_start_date': date(int(yr), int(mo), 1),
                        'written_end_date': (date(int(yr), int(mo), 28) + timedelta(days=4)).replace(day=1) - timedelta(days=1),
                    }
                    try:
                        with_filters_for_dea = tableau.lazyframe_from_view_id(luid, filters=filters)
                        presc_df = pl.concat([presc_df, with_filters_for_dea.collect()])
                    except TableauNoDataError:
                        pass
                fp = Path(f'{row['prescriber_name'].replace(' ', '_')}_filtered_dispensations_{row['MM/YYYY'].replace('/', '-')}.csv')
                presc_df.write_csv(fp)
                all_files.append(fp)
            buff = io.BytesIO()
            with ZipFile(buff, mode='w', compression=ZIP_DEFLATED) as archive:
                for path in all_files:
                    archive.write(path, arcname=path.name)
                    path.unlink()
            buff.seek(0)
            remote_path = f'licensing_boards/{board}/filtered_dispensations/'
            remote_file_path = remote_path + f'{board}_{last_forty.item(1, 'MM/YYYY').replace('/', '-')}.zip'
            sftp.putfo(buff, remote_file_path)
            print(f'{remote_file_path} uploaded to servu')
        remove_oldest_file(sftp, remote_path)
    finally:
        print()
        if sftp:
            sftp.close()
            print('sftp closed')
        if ssh:
            ssh.close()
            print('ssh closed')
        print()
