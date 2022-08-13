import concurrent.futures
import datetime
import io
import sys
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from os.path import exists

import pandas as pd
import requests as requests


def retrieve(year, month, day):
    print("Will retrieve " + str(year) + "-" + str(month) + "-" + str(day))
    url = f"https://fogos.icnf.pt/localizador/webserviceocorrencias.asp" \
          f"?ANO={year}" \
          f"&MES={month}"
    if day is not None:
        url += f"&DIA={day}"

    resp = requests.get(url)

    if resp.status_code == 500:
        raise Exception('Error connecting to ' + url + "\n" + resp.text)

    if "Sistema sem dados!..." not in resp.text:
        et = ET.parse(io.StringIO(resp.text))

        df = pd.DataFrame([
            {f.tag: f.text for f in e.findall('./')} for e in et.findall('./')]
        )
        df = df.reset_index()
        print("Retrieved " + str(len(df)) + " entries for " + str(year) + "-" + str(month) + "-" + str(day))
        return df.replace("|", "\\")
    else:
        return pd.DataFrame()


def concat(df1, df2):
    if len(df1) == 0:
        return df2
    if len(df2) == 0:
        return df1
    merged_df = pd.concat([df1, df2]).drop_duplicates()
    df_sorted = merged_df.sort_values(["ANO", "MES", "DIA"])
    df_sorted.reset_index()
    return df_sorted


def save_year_2_file(df, year):
    filename = "data/" + str(year) + ".csv"
    df.to_csv(filename, index=False, sep="|")
    print("saved file " + filename)


def retrieve_year(year):
    print('Start retrieve ' + str(year))
    dt = datetime.datetime.today()
    current_year = dt.year
    current_month = dt.month
    accumulated_df = None
    for month in range(1, 12 + 1):
        if (year < current_year or (year == current_year and month <= current_month)):
            retrieved_df = retrieve(year, month, None)
            if accumulated_df is None:
                accumulated_df = retrieved_df
            else:
                accumulated_df = concat(accumulated_df, retrieved_df)

    print("Retrieved a total of " + str(len(accumulated_df)) + " for year " + str(year))
    save_year_2_file(accumulated_df, year)


def retrieve_yesterday():
    print('Start retrieve yesterday')
    dt = datetime.datetime.today()
    year = dt.year
    month = dt.month
    day = dt.day - 1

    yesterday_df = retrieve(year, month, day)
    filename = "data/" + str(year) + ".csv"
    if exists(filename):
        year_df = concat(yesterday_df, pd.read_csv(filename))
        save_year_2_file(year_df, year)
    else:
        save_year_2_file(yesterday_df, year)


def retrieve_all():
    print('Start retrieve all')
    with ThreadPoolExecutor(max_workers=6) as executor:
        dt = datetime.datetime.today()
        current_year = dt.year
        jobs = range(2001, current_year+1)
        try:
            futures = []
            for year in jobs:
                futures.append(executor.submit(retrieve_year, year))

            complete_count = 0
            for future in concurrent.futures.as_completed(futures):
                complete_count += 1
                print('Completed tasks: ' + str(complete_count) + ' of ' + str(len(jobs)))
        except Exception as e:
            print("Error: unable to start thread")
            print(e)
        print('End')


if len(sys.argv) <= 1:
    retrieve_yesterday()
elif sys.argv[1] == 'all':
    retrieve_all()
else:
    retrieve_year(int(sys.argv[1]))
