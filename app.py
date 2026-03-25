import json
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import streamlit as st

DATA_DIR = Path(__file__).parent / 'data'
MERGED_FILE = DATA_DIR / 'merged_data.parquet'
CCMAP_FILE = DATA_DIR / 'country_city_map.json'


# ---------------------------------------------------------------------------
# Filter helper
# ---------------------------------------------------------------------------

def apply_filters(data: pd.DataFrame, country: str, city: str,
                  trip_duration, weekday: str,
                  start_date, end_date) -> pd.DataFrame:
    df = data.copy()
    df = df[(df['total_price'] > 0) & (df['price_x'] > 0) & (df['price_y'] > 0)]

    if country != 'All' and city == 'All':
        df = df[df['country_x'] == country]
    elif city != 'All':
        df = df[df['destination_city'] == city]

    df = df[(df['date_x'] >= start_date) & (df['date_y'] <= end_date)]

    if trip_duration != 'All':
        df = df[df['trip_duration'] == trip_duration]

    if weekday != 'All':
        df = df[df['week_day_x'] == weekday]

    return df.drop_duplicates()


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

st.set_page_config(page_title='Flight Finder', page_icon='✈', layout='wide')
st.title('✈ Round-Trip Flight Finder — from Sofia')

# --- Load saved data ---
if 'merged' not in st.session_state:
    if MERGED_FILE.exists() and CCMAP_FILE.exists():
        try:
            st.session_state['merged'] = pd.read_parquet(MERGED_FILE)
            with open(CCMAP_FILE) as f:
                st.session_state['ccmap'] = json.load(f)
        except Exception as e:
            st.error(f'Could not load saved data: {e}')
            st.stop()

if 'merged' not in st.session_state:
    st.info('No data available yet. Run `python fetch_data.py` to populate the data.')
    st.stop()

merged: pd.DataFrame = st.session_state['merged']
ccmap: dict = st.session_state['ccmap']

# --- Sidebar ---
with st.sidebar:
    if MERGED_FILE.exists():
        mtime = datetime.fromtimestamp(MERGED_FILE.stat().st_mtime)
        st.caption(f'Last updated: {mtime.strftime("%d %b %Y %H:%M")}')
    st.caption(f'{len(merged):,} round-trip combinations loaded')

# --- Filters ---
col1, col2, col3 = st.columns(3)
with col1:
    country_options = ['All'] + [k for k in ccmap if k != 'All']
    country = st.selectbox('Country', country_options)
with col2:
    city_options = ['All'] + (ccmap.get(country, []) if country != 'All' else ccmap['All'])
    city = st.selectbox('City', city_options)
with col3:
    sort_by = st.selectbox('Sort by', ['Total Price', 'Outbound Date'])

col4, col5, _ = st.columns(3)
with col4:
    duration_options = ['All'] + sorted(merged['trip_duration'].dropna().unique().astype(int).tolist())
    trip_duration = st.selectbox('Trip Duration (days)', duration_options)
with col5:
    weekday_options = ['All'] + sorted(merged['week_day_x'].dropna().unique().tolist())
    weekday = st.selectbox('Departure Weekday', weekday_options)

all_dates_x = sorted(merged['date_x'].dropna().unique())
all_dates_y = sorted(merged['date_y'].dropna().unique())

col7, col8 = st.columns(2)
with col7:
    default_start = max(all_dates_x[0], date.today())
    start_date = st.date_input('Outbound from', value=default_start,
                               min_value=all_dates_x[0], max_value=all_dates_x[-1])
with col8:
    end_date = st.date_input('Return by', value=all_dates_y[-1],
                             min_value=all_dates_y[0], max_value=all_dates_y[-1])

# --- Apply filters ---
filtered = apply_filters(merged, country, city, trip_duration, weekday, start_date, end_date)

if sort_by == 'Total Price':
    filtered = filtered.sort_values('total_price')
else:
    filtered = filtered.sort_values('date_x')

# --- Metrics ---
m1, m2, m3 = st.columns(3)
m1.metric('Results', f'{len(filtered):,}')
if not filtered.empty:
    m2.metric('Cheapest Trip', f'€{filtered["total_price"].min():.0f}')
    m3.metric('Avg Total Price', f'€{filtered["total_price"].mean():.0f}')

st.divider()

# --- Results table ---
if filtered.empty:
    st.warning('No flights match the current filters.')
else:
    display_df = (filtered
                  .rename(columns={
                      'destination_city': 'Destination',
                      'country_x': 'Country',
                      'trip_duration': 'Days',
                      'total_price': 'Total €',
                      'index_x': 'Out Airline',
                      'date_x': 'Out Date',
                      'departure_time_x': 'Out Time',
                      'week_day_x': 'Out Day',
                      'price_x': 'Out €',
                      'index_y': 'Ret Airline',
                      'date_y': 'Ret Date',
                      'departure_time_y': 'Ret Time',
                      'week_day_y': 'Ret Day',
                      'price_y': 'Ret €',
                  })
                  .set_index('Destination'))

    st.dataframe(
        display_df.head(200),
        use_container_width=True,
        column_config={
            'Total €': st.column_config.NumberColumn(format='€%.0f'),
            'Out €': st.column_config.NumberColumn(format='€%.0f'),
            'Ret €': st.column_config.NumberColumn(format='€%.0f'),
            'Out Date': st.column_config.DateColumn(format='DD MMM YYYY'),
            'Ret Date': st.column_config.DateColumn(format='DD MMM YYYY'),
        },
    )
