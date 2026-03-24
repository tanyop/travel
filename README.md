# Round-Trip Flight Finder — from Sofia

Streamlit app that finds cheap round trips from Sofia (SOF) across RyanAir and WizzAir destinations.

## Features

- Fetches cheapest-per-day fares from both airlines for the next 3 months
- Combines outbound + inbound into round-trip combinations with total price
- Filters by country, city, trip duration, departure weekday, and date range
- Data updated 3× daily via GitHub Actions (06:00, 12:00, 18:00 UTC)

## Project structure

```
app.py              # Streamlit UI
flights_data.py     # RyanAirData, WizzAirData, FlightsData classes
fetch_data.py       # Headless fetch script — saves data to disk
data/
  merged_data.parquet
  country_city_map.json
.github/workflows/
  fetch_data.yml    # GitHub Actions cron job
```

## Setup

```bash
pip install -r requirements.txt
```

## Populate data

```bash
python fetch_data.py
```

This fetches the next 3 months from both airlines and saves results to `data/`. Takes several minutes due to WizzAir rate limiting (3s delay per destination).

## Run the app

```bash
streamlit run app.py
```

## Deployment

Hosted on [Streamlit Community Cloud](https://streamlit.io/cloud). GitHub Actions runs `fetch_data.py` three times a day and commits updated data files back to the repo, triggering an automatic redeploy.

**Required GitHub setting:** Settings → Actions → General → Workflow permissions → Read and write.
