import datetime
import sys

l_year = 2025
today = datetime.date.today()
year = today.year


def check_license():
    if year > l_year:
        sys.exit(0)
    else:
        pass
