"""
Fetch flight data for the next 3 months and save to disk.
Run this script 3 times a day via Task Scheduler (Windows) or cron (Linux/Mac).

Windows Task Scheduler command:
    python C:/code/travel/fetch_data.py

Cron (06:00, 12:00 and 18:00 UTC):
    0 6,12,18 * * * /usr/bin/python3 /code/travel/fetch_data.py
"""

import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path

from flights_data import FlightsData

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler('fetch_data.log'),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / 'data'
MERGED_FILE = DATA_DIR / 'merged_data.parquet'
CCMAP_FILE = DATA_DIR / 'country_city_map.json'


def months_to_fetch() -> list[str]:
    month_order = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                   'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    today = date.today()
    return [month_order[(today.month - 1 + i) % 12] for i in range(0, 4)]


def main():
    months = months_to_fetch()
    log.info(f'Fetching data for {months}')

    DATA_DIR.mkdir(exist_ok=True)

    fd = FlightsData(months=months, origin='SOF')
    fd.fetch_data()

    merged = fd.merged_data
    ccmap = fd.country_city_map

    merged.to_parquet(MERGED_FILE)
    ccmap['_last_updated'] = datetime.now(timezone.utc).strftime('%d %b %Y %H:%M UTC')
    with open(CCMAP_FILE, 'w') as f:
        json.dump(ccmap, f)

    log.info(f'Saved {len(merged):,} combinations to {MERGED_FILE}')


if __name__ == '__main__':
    main()
