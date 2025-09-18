import sys
from datetime import datetime
from pathlib import Path

from utils.constants import PHX_TZ


def warn_file_age(file: Path) -> None:
    """
    warns if file age of input file is older than 24 hours and prompts the user for if they want to continue with the script

    args:
        file: Path to the file in question
    """
    file_age = datetime.now(tz=PHX_TZ) - datetime.fromtimestamp(file.stat().st_mtime, tz=PHX_TZ)
    if file_age.days > 1:
        print(f'warning: `{file}` has not been updated recently!')
        print(f'the file is {file_age.days} days and {round(file_age.seconds / 60 / 60, 2)} hours old')
        print('please consider updating it and running this script again')
        print()
        answer = input('proceed? (y to continue with old file): ')
        if answer != 'y':
            sys.exit('update files and run again')
