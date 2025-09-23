import sys
import warnings
from datetime import datetime
from pathlib import Path

from utils.constants import PHX_TZ


def warn_file_age(file: Path, max_age_days: int = 1) -> None:
    """
    warns if file age of input file is older than 24 hours and prompts the user for if they want to continue with the script

    args:
        file: Path to the file in question
        max_age_days: the max age in days the file can be before triggering this warning
    """
    file_age = datetime.now(tz=PHX_TZ) - datetime.fromtimestamp(file.stat().st_mtime, tz=PHX_TZ)
    if file_age.days > max_age_days:
        msg = (
            f'`{file}` has not been updated recently!\n'
            f'the file is {file_age.days} days and {round(file_age.seconds / 60 / 60, 2)} hours old\n'
            f'please consider updating it and running this script again\n'
        )
        warnings.warn(msg, UserWarning, stacklevel=2)
        answer = input('proceed? (y to continue with old file): ')
        if answer != 'y':
            sys.exit('update files and run again')
