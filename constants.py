from zoneinfo import ZoneInfo

MAX_SERVU_FILE_COUNT = 5                                    # the max number of files to keep on the servu
PHX_TZ = ZoneInfo('America/Phoenix')                        # phoenix timezone
TOP_PRESCRIBERS = 40                                        # number of prescribers with the most dispensations and no searches for mandatory use reporting

DAILY_DAYS_DELINQUENT_THRESHOLD = 2                         # min days delinquent to receive daily notices
WEEKLY_DAYS_DELINQUENT_THRESHOLD = 7                        # min days delinquent to receive weekly notices
MAX_DAYS_EXCUSED = 6                                        # max number of days that can be excused for data submitters
