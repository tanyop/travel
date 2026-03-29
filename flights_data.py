import time
import warnings
from datetime import date, datetime

import pandas as pd
import requests
from tqdm import tqdm

warnings.filterwarnings('ignore', message='Unverified HTTPS request')

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

destinations_country_map = {
    'uz': 'Uzbekistan', 'ae': 'United Arab Emirates', 'si': 'Slovenia',
    'sa': 'Saudi Arabia', 'om': 'Oman', 'md': 'Moldova', 'mv': 'Maldives',
    'mk': 'Macedonia', 'kg': 'Kyrgyz', 'xk': 'Kosovo', 'kz': 'Kazakhstan',
    'is': 'Iceland', 'ge': 'Georgia', 'eg': 'Egypt', 'az': 'Azerbaijan',
    'am': 'Armenia', 'dk': 'Denmark', 'gb': 'Great Britain', 'es': 'Spain',
    'ma': 'Marocco', 'jo': 'Jordan', 'nl': 'Netherlands', 'se': 'Sweden',
    'gr': 'Greece', 'de': 'Germany', 'fr': 'France', 'tr': 'Turkey',
    'ba': 'Bosnia and Herzegovina', 'bg': 'Bulgaria', 'cz': 'Czechia',
    'be': 'Belgium', 'ch': 'Switzerland', 'sk': 'Slovakia', 'hu': 'Hungary',
    'pl': 'Poland', 'ro': 'Romania', 'hr': 'Croatia', 'ie': 'Ireland',
    'pt': 'Portugal', 'fi': 'Finland', 'rs': 'Serbia', 'at': 'Austria',
    'lt': 'Lithuania', 'cy': 'Cyprus', 'lu': 'Luxembourg', 'mt': 'Malta',
    'no': 'Norway', 'lv': 'Latvia', 'me': 'Montenegro', 'al': 'Albania',
    'ee': 'Estonia', 'il': 'Israel', 'it': 'Italy',
}

months_dates = {
    'Jan': {'start_date': '2026-01-01', 'end_date': '2026-01-31'},
    'Feb': {'start_date': '2026-02-01', 'end_date': '2026-02-28'},
    'Mar': {'start_date': '2026-03-01', 'end_date': '2026-03-31'},
    'Apr': {'start_date': '2026-04-01', 'end_date': '2026-04-30'},
    'May': {'start_date': '2026-05-01', 'end_date': '2026-05-31'},
    'Jun': {'start_date': '2026-06-01', 'end_date': '2026-06-30'},
    'Jul': {'start_date': '2026-07-01', 'end_date': '2026-07-31'},
    'Aug': {'start_date': '2026-08-01', 'end_date': '2026-08-31'},
    'Sep': {'start_date': '2026-09-01', 'end_date': '2026-09-30'},
    'Oct': {'start_date': '2026-10-01', 'end_date': '2026-10-31'},
    'Nov': {'start_date': '2026-11-01', 'end_date': '2026-11-30'},
    'Dec': {'start_date': '2026-12-01', 'end_date': '2026-12-31'},
}

wizz_headers = {
    'authority': 'be.wizzair.com',
    'accept': 'application/json, text/plain, */*',
    'origin': 'https://wizzair.com',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36',
    'content-type': 'application/json;charset=UTF-8',
    'sec-fetch-site': 'same-site',
    'sec-fetch-mode': 'cors',
    'referer': 'https://wizzair.com/en-gb/flights/timetable',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'en-GB,en;q=0.9,en-US;q=0.8',
}

conv_to_eur = {
    'ALL': 0.01, 'USD': 0.85, 'EUR': 1, 'BAM': 0.51, 'BGN': 0.51,
    'CZK': 0.041, 'DKK': 0.13, 'GEL': 0.32, 'HUF': 0.0025, 'ILS': 0.26,
    'MKD': 0.016, 'NOK': 0.085, 'PLN': 0.24, 'RON': 0.1969, 'RSD': 0.0085,
    'SEK': 0.091, 'CHF': 1.07, 'AED': 0.23, 'GBP': 1.15,
}

COUNTRY_NAME_NORMALIZATIONS = {
    'United Kingdom': 'Great Britain',
}

CITY_NAME_NORMALIZATIONS = {
    'Rome Fiumicino': 'Rome', 'Rome Ciampino': 'Rome', 'Rome (All Airports)': 'Rome',
    'Roma (Toate aeroporturile)': 'Rome', 'Roma Ciampino': 'Rome', 'Roma Fiumicino': 'Rome',
    'Naples Salerno': 'Naples', 'Napoli': 'Naples', 'Lamezia Terme': 'Lamezia', 'Nisa': 'Nice', 'Paris Beauvais': 'Paris', 'Paris (Toate aeroporturile)': 'Paris',
    'Paris (All Airports)': 'Paris',
    'London Luton': 'London', 'London (All Airports)': 'London',
    'Londra (toate aeroporturile)': 'London', 'Londra Luton': 'London',
    'Brussels Charleroi': 'Charleroi', 'Bruxelles Charleroi': 'Charleroi', 'Milan Bergamo': 'Bergamo', 'Milan (All Airports)': 'Bergamo',
    'Milano (toate aeroporturile)': 'Bergamo', 'Milano Bergamo': 'Bergamo',
    'Barcelona El Prat': 'Barcelona', 'Memmingen / Munich West': 'Memmingen',
    'Memmingen / Munchen Vest': 'Memmingen', 'Memmingen / München Vest': 'Memmingen',
    'Frankfurt Hahn': 'Frankfurt', 'Kraków\r\n': 'Krakow', 'Kraków': 'Krakow',
    'Palma de Mallorca': 'Palma', 'Tel-Aviv': 'Tel Aviv', 'Málaga': 'Malaga',
    'Warsaw Modlin': 'Warsaw', 'Varșovia Chopin': 'Warsaw', 'Varșovia Modlin': 'Warsaw',
    'Lyon\r\n': 'Lyon', 'Marrakesh\r\n': 'Marrakesh',
}


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

class RyanAirData:

    def __init__(self, origin: str = 'SOF'):
        self.origin = origin
        self._destinations_info: pd.DataFrame = None

    @property
    def destinations_info(self) -> pd.DataFrame:
        if self._destinations_info is None:
            url = f'https://www.ryanair.com/api/views/locate/searchWidget/routes/en/airport/{self.origin}'
            r = requests.get(url)
            r.raise_for_status()
            rows = []
            for dest in r.json():
                rows.append({
                    'code': dest['arrivalAirport']['code'],
                    'country': dest['arrivalAirport']['country']['name'],
                    'city': dest['arrivalAirport']['city']['name'],
                    'ccy': dest['arrivalAirport']['country']['currency'],
                })
            self._destinations_info = pd.DataFrame(rows).set_index('code')
        return self._destinations_info

    @property
    def destinations_codes(self) -> dict:
        return self.destinations_info['city'].to_dict()

    @property
    def all_destinations(self) -> pd.DataFrame:
        r = requests.get('https://www.ryanair.com/api/views/locate/3/airports/en/active')
        r.raise_for_status()
        all_dest = pd.json_normalize(r.json())
        all_dest = (all_dest[['iataCode', 'name', 'countryCode', 'currencyCode']]
                    .rename(columns={'iataCode': 'code', 'name': 'city',
                                     'countryCode': 'country', 'currencyCode': 'ccy'})
                    .set_index('city'))
        all_dest['country'] = all_dest['country'].map(destinations_country_map)
        return all_dest

    def _fetch_single(self, origin: str, destination: str, start_date: str) -> dict:
        url = (f'https://www.ryanair.com/api/farfnd/v4/oneWayFares/{origin}/{destination}'
               f'/cheapestPerDay?OutboundMonthOfDate={start_date}&currency=EUR')
        r = requests.get(url)
        r.raise_for_status()
        return r.json()

    def _parse_data(self, origin: str, destination: str, api_output: dict) -> pd.DataFrame:
        rows = []
        for fare in api_output['outbound']['fares']:
            if fare['price'] is not None:
                rows.append({
                    'origin_code': origin,
                    'destination_code': destination,
                    'departure_date': fare['departureDate'],
                    'price': fare['price']['value'],
                })
        return pd.DataFrame(rows, index=['RyanAir'] * len(rows))

    def fetch_single_destination(self, origin: str, destination: str, start_date: str) -> pd.DataFrame:
        api_output = self._fetch_single(origin, destination, start_date)
        if api_output:
            return self._parse_data(origin, destination, api_output)
        return pd.DataFrame()

    def fetch_all_destinations(self, start_date: str) -> pd.DataFrame:
        dest_info = self.destinations_info  # fetch once, reuse
        dest_codes = self.destinations_codes
        month_label = datetime.strptime(start_date, '%Y-%m-%d').strftime('%b')

        frames = []
        for destination in tqdm(dest_codes.keys(), desc=f'RyanAir {month_label}'):
            try:
                out_df = self.fetch_single_destination(self.origin, destination, start_date)
                ret_df = self.fetch_single_destination(destination, self.origin, start_date)
                for df in (out_df, ret_df):
                    if not df.empty:
                        df['destination_city'] = dest_codes[destination]
                        df['country'] = dest_info.loc[destination, 'country']
                        frames.append(df)
            except Exception as err:
                print(f'RyanAir error {destination}: {err}')

        if not frames:
            return pd.DataFrame()

        flights = pd.concat(frames)
        flights['date'] = pd.to_datetime(flights['departure_date'].apply(
            lambda x: datetime.fromisoformat(x).date()))
        flights['week_day'] = flights['date'].dt.day_name()
        flights['departure_time'] = flights['departure_date'].apply(
            lambda x: datetime.fromisoformat(x).time())
        flights = flights.drop(columns=['departure_date'])
        return flights[['origin_code', 'destination_code', 'destination_city',
                        'price', 'date', 'week_day', 'departure_time', 'country']]


class WizzAirData:

    def __init__(self, origin: str = 'SOF') -> None:
        self.origin = origin
        self.api_version = self._get_version()
        self._destination_codes: dict = None
        self._dest_info: pd.DataFrame = pd.DataFrame()
        self._exclude: list = []

    def _get_version(self) -> str:
        version = requests.get('https://www.wizzair.com/buildnumber').text
        ind = version.find('com/')
        return version[ind + 4:]

    def _get_dest_info(self) -> pd.DataFrame:
        url = f'https://be.wizzair.com/{self.api_version}/Api/asset/map?languageCode=eng-gb'
        r = requests.get(url, headers=wizz_headers, verify=False)
        r.raise_for_status()
        w_dest = pd.json_normalize(r.json()['cities'])
        w_dest = (w_dest[['iata', 'shortName', 'countryCode', 'currencyCode', 'connections']]
                  .rename(columns={'iata': 'code', 'shortName': 'name',
                                   'countryCode': 'country', 'currencyCode': 'ccy'})
                  .set_index('code'))
        w_dest['country'] = w_dest['country'].str.lower().map(destinations_country_map)
        w_dest['connections'] = [[d['iata'] for d in conns] for conns in w_dest['connections']]
        self._exclude = w_dest[w_dest['name'].str.contains('Any')].index.tolist()
        return w_dest[~w_dest['name'].str.contains('Any')]

    @property
    def destinations_info(self) -> pd.DataFrame:
        if self._dest_info.empty:
            self._dest_info = self._get_dest_info()
        conns = [c for c in self._dest_info.loc[self.origin, 'connections']
                 if c not in self._exclude]
        return self._dest_info.loc[conns].drop(columns='connections')

    @property
    def all_destinations(self) -> pd.DataFrame:
        if self._dest_info.empty:
            self._dest_info = self._get_dest_info()
        return self._dest_info.drop(columns='connections').reset_index().set_index('name')

    @property
    def destinations_codes(self) -> dict:
        if self._destination_codes is None:
            self._destination_codes = self.destinations_info['name'].to_dict()
        return self._destination_codes

    def _fetch_single(self, origin: str, destination: str,
                      start_date: str, end_date: str) -> dict:
        body = {
            'adultCount': 1, 'childCount': 0, 'infantCount': 0,
            'flightList': [
                {'departureStation': origin, 'arrivalStation': destination,
                 'from': start_date, 'to': end_date},
                {'departureStation': destination, 'arrivalStation': origin,
                 'from': start_date, 'to': end_date},
            ],
            'priceType': 'regular',
        }
        url = f'https://be.wizzair.com/{self.api_version}/Api/search/timetable'
        r = requests.post(url, json=body, headers=wizz_headers, verify=False)
        r.raise_for_status()
        return r.json()

    def _parse_data(self, api_output: dict, return_flight: bool = False) -> pd.DataFrame:
        col = 'returnFlights' if return_flight else 'outboundFlights'
        df = pd.json_normalize(api_output[col])[
            ['departureStation', 'arrivalStation', 'departureDates',
             'price.amount', 'price.currencyCode']]
        df.columns = ['origin_code', 'destination_code', 'departure_date', 'price', 'currency']
        df.index = ['WizzAir'] * len(df)
        df['departure_date'] = df['departure_date'].apply(lambda x: x[0])
        conv_rate = conv_to_eur[df['currency'].iloc[0]]
        df['price'] = (df['price'] * conv_rate).round(2)
        return df.drop(columns='currency')

    def fetch_single_destination(self, origin: str, destination: str,
                                 start_date: str, end_date: str) -> pd.DataFrame | None:
        api_output = self._fetch_single(origin, destination, start_date, end_date)
        if not api_output['outboundFlights'] or not api_output['returnFlights']:
            print(f'No WizzAir data for {destination}')
            return None
        outbound = self._parse_data(api_output, return_flight=False)
        inbound = self._parse_data(api_output, return_flight=True)
        city = self.destinations_codes[destination]
        country = self.destinations_info.loc[destination, 'country']
        for df in (outbound, inbound):
            df['destination_city'] = city
            df['country'] = country
        return pd.concat([outbound, inbound])

    def fetch_all_destinations(self, start_date: str, end_date: str) -> pd.DataFrame:
        month_label = datetime.strptime(start_date, '%Y-%m-%d').strftime('%b')
        frames = []
        for dest_code in tqdm(self.destinations_codes, desc=f'WizzAir {month_label}'):
            dest_data = self.fetch_single_destination(
                self.origin, dest_code, start_date, end_date)
            if dest_data is not None:
                frames.append(dest_data)
            time.sleep(3)

        if not frames:
            return pd.DataFrame()

        all_data = pd.concat(frames)
        all_data['date'] = pd.to_datetime(all_data['departure_date'].apply(
            lambda x: datetime.fromisoformat(x).date()))
        all_data['week_day'] = all_data['date'].dt.day_name()
        all_data['departure_time'] = all_data['departure_date'].apply(
            lambda x: datetime.fromisoformat(x).time())
        all_data = all_data.drop(columns=['departure_date'])
        return all_data[['origin_code', 'destination_code', 'destination_city',
                         'price', 'date', 'week_day', 'departure_time', 'country']]


class FlightsData:

    def __init__(self, months: list, origin: str = 'SOF') -> None:
        self.origin = origin
        self.months = months
        self.ryan_air_data: pd.DataFrame = pd.DataFrame()
        self.wizz_air_data: pd.DataFrame = pd.DataFrame()
        self._flights_data: pd.DataFrame = None
        self._merged_data: pd.DataFrame = None
        self._country_city_map: dict = None

    def fetch_data(self) -> None:
        ra = RyanAirData(origin=self.origin)
        wa = WizzAirData(origin=self.origin)

        ra_codes = set(ra.all_destinations['code'].values)
        wa_codes = set(wa.all_destinations.reset_index()['code'].values
                       if 'code' in wa.all_destinations.columns
                       else wa.all_destinations.index)

        origin_in_ra = self.origin in ra_codes
        origin_in_wa = self.origin in wa_codes

        ra_frames, wa_frames = [], []

        for month in self.months:
            if month == date.today().strftime('%B')[:3]:
                start_date = date.today().strftime('%Y-%m-%d')
            else:
                start_date = months_dates[month]['start_date']
            end_date = months_dates[month]['end_date']

            if origin_in_ra:
                df = ra.fetch_all_destinations(start_date=start_date)
                if not df.empty:
                    ra_frames.append(df)

            if origin_in_wa:
                df = wa.fetch_all_destinations(start_date=start_date, end_date=end_date)
                if not df.empty:
                    wa_frames.append(df)

        if ra_frames:
            self.ryan_air_data = pd.concat(ra_frames, ignore_index=False)
        if wa_frames:
            self.wizz_air_data = pd.concat(wa_frames, ignore_index=False)

    @property
    def flights_data(self) -> pd.DataFrame:
        if self._flights_data is None:
            frames = [df for df in (self.ryan_air_data, self.wizz_air_data) if not df.empty]
            self._flights_data = pd.concat(frames) if frames else pd.DataFrame()
            self._flights_data['destination_city'] = (
                self._flights_data['destination_city'].str.strip()
                .replace(CITY_NAME_NORMALIZATIONS))
            self._flights_data['country'] = (
                self._flights_data['country'].replace(COUNTRY_NAME_NORMALIZATIONS))
        return self._flights_data

    @property
    def merged_data(self) -> pd.DataFrame:
        if self._merged_data is None:
            fd = self.flights_data
            out_data = fd[fd['origin_code'] == self.origin].reset_index()
            in_data = fd[fd['destination_code'] == self.origin].reset_index()

            merged = out_data.merge(in_data, on='destination_city', how='left',
                                    suffixes=('_x', '_y'))
            merged = merged.drop(columns=[
                'origin_code_y', 'destination_code_y', 'country_y',
                'origin_code_x', 'destination_code_x'])
            merged['total_price'] = merged['price_x'] + merged['price_y']
            merged['trip_duration'] = (merged['date_y'] - merged['date_x']).dt.days
            merged['date_x'] = merged['date_x'].dt.date
            merged['date_y'] = merged['date_y'].dt.date
            merged = merged[merged['trip_duration'] >= 0]
            same_day = merged['date_x'] == merged['date_y']
            merged = merged[~same_day | (merged['departure_time_x'] < merged['departure_time_y'])]
            self._merged_data = merged[[
                'destination_city', 'country_x', 'trip_duration', 'total_price',
                'index_x', 'date_x', 'departure_time_x', 'week_day_x', 'price_x',
                'index_y', 'date_y', 'departure_time_y', 'week_day_y', 'price_y',
            ]]
        return self._merged_data

    @property
    def country_city_map(self) -> dict:
        if self._country_city_map is None:
            fd = self.flights_data
            self._country_city_map = {
                country: sorted(fd[fd['country'] == country]['destination_city'].unique().tolist())
                for country in sorted(fd['country'].dropna().unique())
            }
            self._country_city_map['All'] = sorted(fd['destination_city'].unique().tolist())
        return self._country_city_map
