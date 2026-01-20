from datetime import date
from zoneinfo import ZoneInfo

WEEKLY_DAYS_DELINQUENT_THRESHOLD = 7                        # min days delinquent to receive weekly notices
DAILY_DAYS_DELINQUENT_THRESHOLD = 2                         # min days delinquent to receive daily notices
EARLIEST_AWARXE_DATE = date(year=2022, month=12, day=7)     # the date of the earliest awarxe file
MAX_SERVU_FILE_COUNT = 5                                    # the max number of files to keep on the servu
PHX_TZ = ZoneInfo('America/Phoenix')                        # phoenix timezone
TOP_PRESCRIBERS = 40                                        # number of prescribers with the most dispensations and no searches for mandatory use reporting
AZ_HOLIDAYS = {                                             # arizona holidays to exclude from business day calculations
    date(year=2025, month=1, day=1),
    date(year=2025, month=1, day=20),
    date(year=2025, month=2, day=17),
    date(year=2025, month=5, day=26),
    date(year=2025, month=7, day=4),
    date(year=2025, month=9, day=1),
    date(year=2025, month=10, day=13),
    date(year=2025, month=11, day=11),
    date(year=2025, month=11, day=27),
    date(year=2025, month=12, day=25),
    date(year=2026, month=1, day=1),
    date(year=2026, month=1, day=19),
    date(year=2026, month=2, day=16),
    date(year=2026, month=5, day=25),
    date(year=2026, month=7, day=3),
    date(year=2026, month=9, day=7),
    date(year=2026, month=10, day=12),
    date(year=2026, month=11, day=11),
    date(year=2026, month=11, day=26),
    date(year=2026, month=12, day=25),
}
