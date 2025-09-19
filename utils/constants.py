from datetime import date
from zoneinfo import ZoneInfo

DAYS_DELINQUENT_THRESHOLD = 7                               # min days delinquent that will put a dispenser on the weekly pharmacy cleanup report
EARLIEST_AWARXE_DATE = date(year=2022, month=12, day=7)     # the date of the earliest awarxe file
JULY_MONTH_NUMBER = 7
MAX_SERVU_FILE_COUNT = 5                                    # the max number of files to keep on the servu
PHX_TZ = ZoneInfo('America/Phoenix')                        # phoenix timezone
TOP_PRESCRIBERS = 20                                        # number of prescribers with the most dispensations and no searches for mandatory use reporting
