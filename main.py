import pandas as pd
import requests
import io

from datetime import date, timedelta
from time import time, sleep
import random

from tqdm import tqdm
# import click

from joblib import Parallel, delayed




def get_download_url(top_type='regional',
                     country='global',
                     time_period_type='weekly',
                     time_period='latest'):
    return f'https://spotifycharts.com/{top_type}/{country}/{time_period_type}/{get_url_time_period(time_period)}/download'


all_possible_countries = [
    "global", "us", "gb", "ad", "ar", "at", "au", "be", "bg", "bo", "br", "ca",
    "ch", "cl", "co", "cr", "cy", "cz", "de", "dk", "do", "ec", "ee", "es",
    "fi", "fr", "gr", "gt", "hk", "hn", "hu", "id", "ie", "il", "in", "is",
    "it", "jp", "lt", "lu", "lv", "mc", "mt", "mx", "my", "ni", "nl", "no",
    "nz", "pa", "pe", "ph", "pl", "pt", "py", "ro", "se", "sg", "sk", "sv",
    "th", "tr", "tw", "uy", "vn", "za"
]

# https://spotifycharts.com/regional/global/daily/latest/download
# https://spotifycharts.com/regional/global/weekly/latest/download
# https://spotifycharts.com/regional/global/weekly/2019-09-06--2019-09-13/download
# https://spotifycharts.com/regional/au/weekly/2019-10-04--2019-10-11/download
# https://spotifycharts.com/regional/au/daily/2019-10-07/download

# https://spotifycharts.com/regional/global/weekly/2016-12-23--2016-12-30/download
# https://spotifycharts.com/regional/global/weekly/2016-12-30--2017-01-06/download
# https://spotifycharts.com/regional/global/weekly/2017-01-06--2017-01-13/download


# @click.command()
# @click.argument('--')
def generate_time_periods():

    initial_date = date(2016, 12, 23)
    # if daily
    # initial_date = date(2017,1,1)

    next_date = timedelta(days=7)

    temp_date = initial_date
    lst = []

    while True:
        lst.append(temp_date)
        temp_date += next_date

        if temp_date > date.today():
            break

    return list(zip(lst, lst[1:]))  # weekly pairs


# @click.argument('--countries')
def generate_countries_list(country='all'):
    if country == 'all':
        return all_possible_countries
    elif country in all_possible_countries:
        return [country]
    else:
        raise ValueError(
            f"Invalid country (generate_countries_list) - {country}")


def download_csv(url):

    r = requests.get(url)

    #random pause
    # sleep(random.random() * 3)

    if r.status_code != 200:
        print("Error on download_csv - {}".format(url))

    csv_binary = r.content.decode("utf-8")

    return io.StringIO(csv_binary)


def get_df(url, country, time_period):
    try:
        df = pd.read_csv(download_csv(url), header=1)
    except Exception as e:
        print(f'Error while reading csv - get_df - {e}')
        # so that the program doesn't stop
        return pd.DataFrame()

    df['Country'] = country

    if isinstance(time_period, tuple) and len(time_period) == 2:
        df['Start Date'] = time_period[0]
        df['End Date'] = time_period[1]
    elif isinstance(time_period, date):
        df['Date'] = time_period
    else:
        raise ValueError(
            f"Error on get_df, time_period invalid - {time_period}")

    return df


def get_url_time_period(date_period):

    if date_period == 'latest':
        return 'latest'
    elif isinstance(date_period, tuple) and len(date_period) == 2:
        # is weekly date
        return f"{date_period[0].strftime('%Y-%m-%d')}--{date_period[1].strftime('%Y-%m-%d')}"
    elif isinstance(date_period, date):
        return f"{date_period.strftime('%Y-%m-%d')}"
    else:
        raise Exception(f"Error on get_url_time_period - {date_period}")


def save_csv(df):
    with open('output.csv', 'w') as f:
        df.to_csv(f, index=False)


def main():

    time_period_type = 'weekly'
    top_type = 'regional'

    time_periods = generate_time_periods()

    countries = generate_countries_list('all')

    dfs_list=Parallel(n_jobs=-1)(
        delayed(get_df)(get_download_url(top_type, country, time_period_type,
                                         time_period), country, time_period)
        for country in tqdm(countries, desc='Country')
        for time_period in tqdm(time_periods, desc='Time Period'))

    final_df = pd.concat(dfs_list, ignore_index=True)

    save_csv(final_df)


if __name__ == "__main__":
    main()