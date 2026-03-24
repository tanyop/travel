# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Running the app

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Architecture

The codebase is split into two files:

- **`flights_data.py`** — all data logic, no Streamlit dependency. Contains three classes and supporting constants:
  - `RyanAirData` — fetches cheapest-per-day fares from the RyanAir public API. Caches `destinations_info` as an instance property to avoid redundant HTTP calls within a single fetch run.
  - `WizzAirData` — fetches timetable data from the WizzAir API (requires SSL verify=False). Auto-detects the API version from `wizzair.com/buildnumber`. Sleeps 3s between destination requests to avoid rate-limiting.
  - `FlightsData` — orchestrates both airlines for a list of months. `fetch_data()` populates `ryan_air_data` / `wizz_air_data`; the `merged_data` property then cross-joins outbound and inbound flights by destination to produce round-trip combinations with `total_price` and `trip_duration`.

- **`app.py`** — Streamlit UI only. Imports `FlightsData` from `flights_data`. Uses `@st.cache_data` keyed on `(months: tuple, origin: str)` to avoid re-fetching on UI interactions. Data is stored in `st.session_state` after the first fetch.

## Key data flow

1. `FlightsData.fetch_data()` → calls `RyanAirData.fetch_all_destinations()` and `WizzAirData.fetch_all_destinations()` per selected month
2. `FlightsData.flights_data` (property) → concatenates both airlines, normalizes city name variants via `CITY_NAME_NORMALIZATIONS`
3. `FlightsData.merged_data` (property) → left-joins outbound flights (origin=SOF) with inbound flights (destination=SOF) on `destination_city`; result has `_x` suffix for outbound and `_y` suffix for inbound columns
4. `app.py:apply_filters()` → filters the merged DataFrame by country, city, trip duration, weekday, and date range before display

## Data notes

- All prices are converted to EUR at fetch time using `conv_to_eur` rates in `flights_data.py`
- `months_dates` hardcodes 2025/2026 date ranges — update annually
- The index of flight DataFrames is the airline name (`'RyanAir'` / `'WizzAir'`), which surfaces as `index_x` / `index_y` in the merged output and is shown as "Out Airline" / "Ret Airline" in the UI
