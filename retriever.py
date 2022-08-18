import concurrent.futures
import datetime
import io
import sys
import threading
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from os.path import exists

import pandas as pd
import requests as requests

IDENTIFIER_COLUMNS = ["ANO", "MES", "DIA", "HORA", "X", "Y", "DATAALERTA", "HORAALERTA"]


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
        return df.replace("|", "/")
    else:
        return pd.DataFrame()


def concat(df1, df2):
    if len(df1) == 0:
        return df2
    if len(df2) == 0:
        return df1
    merged_df = pd.concat([df1, df2]).drop_duplicates(subset=IDENTIFIER_COLUMNS)
    df_sorted = merged_df.sort_values(IDENTIFIER_COLUMNS)
    df_sorted.reset_index()
    return df_sorted


def save_year_2_file(df, year, lock):
    filename = "data/" + str(year) + ".csv"
    df = df.drop("index", axis=1)
    lock.acquire()
    df.to_csv(filename, index=False, sep="|")
    lock.release()
    print("Saved " + str(len(df)) + " entries into " + filename)


def read_year_from_file(year, lock):
    filename = "data/" + str(year) + ".csv"
    lock.acquire()
    if exists(filename):
        df = pd.read_csv(filename, header=0, sep='|', low_memory=False)
        print("Read " + str(len(df)) + " entries from " + filename)
        lock.release()
        return df
    else:
        lock.release()
        return None


def retrieve_year(year, lock):
    print('Start retrieve year ' + str(year))
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
    save_year_2_file(accumulated_df, year, lock)
    print("Retrieved a total of " + str(len(accumulated_df)) + " for year " + str(year))


def retrieve_month(year, month, lock):
    df = retrieve(year, month, None)
    year_df = read_year_from_file(year, lock)
    if year_df is not None:
        year_df = concat(df, year_df)
        save_year_2_file(year_df, year, lock)
    else:
        save_year_2_file(df, year, lock)


def retrieve_day(day, month, year, lock):
    df = retrieve(year, month, day)
    year_df = read_year_from_file(year, lock)
    if year_df is not None:
        year_df = concat(df, year_df)
        save_year_2_file(year_df, year, lock)
    else:
        save_year_2_file(df, year, lock)


def retrieve_last_days(ndays=10):
    print('Start retrieve last ' + str(ndays) + ' days')
    today = datetime.datetime.today()
    fromday = today - datetime.timedelta(days=ndays)
    lock = threading.Lock()
    if (today - fromday).days > 10:
        with ThreadPoolExecutor(max_workers=6) as executor:
            job_date = date(fromday.year, fromday.month, fromday.day)
            begin_of_month = date(today.year, today.month, today.day)
            jobs = []
            while job_date <= begin_of_month:
                jobs.append([job_date.year,job_date.month])
                if (job_date.month < 12):
                    job_date = date(job_date.year, job_date.month + 1, 1)
                else:
                    job_date = date(job_date.year + 1, 1, 1)
            try:
                futures = []
                for year_month in jobs:
                    futures.append(executor.submit(retrieve_month, year_month[0], year_month[1], lock))

                complete_count = 0
                for future in concurrent.futures.as_completed(futures):
                    complete_count += 1
                    print('Completed tasks: ' + str(complete_count) + ' of ' + str(len(jobs)))
            except Exception as e:
                print("Error: unable to start thread")
                print(e)
    else:
        for n in range(1, ndays + 1):
            dt = today - datetime.timedelta(days=n)
            year = dt.year
            month = dt.month
            day = dt.day
            retrieve_day(day, month, year, lock)
    print('End retrieve last ' + str(ndays) + ' days')


def retrieve_all():
    print('Start retrieve all')
    with ThreadPoolExecutor(max_workers=6) as executor:
        dt = datetime.datetime.today()
        current_year = dt.year
        jobs = range(2001, current_year + 1)
        lock = threading.Lock()
        try:
            futures = []
            for year in jobs:
                futures.append(executor.submit(retrieve_year, year, lock))

            complete_count = 0
            for future in concurrent.futures.as_completed(futures):
                complete_count += 1
                print('Completed tasks: ' + str(complete_count) + ' of ' + str(len(jobs)))
        except Exception as e:
            print("Error: unable to start thread")
            print(e)
        print('End')


if sys.argv[1] == 'ndays':
    retrieve_last_days(int(sys.argv[2]))
elif sys.argv[1] == 'all':
    retrieve_all()
else:
    retrieve_year(int(sys.argv[1]), threading.Lock())
