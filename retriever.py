import concurrent.futures
import datetime
import io
import sys
import threading
import xml.etree.ElementTree as ET
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from os.path import exists

import pandas as pd
import requests as requests

IDENTIFIER_COLUMNS = ["id"]
ALL_COLUMNS = ["id", "DISTRITO", "TIPO", "ANO", "AREAPOV", "AREAMATO", "AREAAGRIC", "AREATOTAL", "REACENDIMENTOS", "QUEIMADA", "FALSOALARME",
               "FOGACHO", "INCENDIO", "AGRICOLA", "NCCO", "NOMECCO", "DATAALERTA", "HORAALERTA", "LOCAL", "CONCELHO", "FREGUESIA",
               "FONTEALERTA", "INE", "X", "Y", "DIA", "MES", "HORA", "OPERADOR", "PERIMETRO", "APS", "CAUSA", "TIPOCAUSA", "DHINICIO",
               "DHFIM", "DURACAO", "HAHORA", "DATAEXTINCAO", "HORAEXTINCAO", "DATA1INTERVENCAO", "HORA1INTERVENCAO", "QUEIMA", "LAT",
               "LON", "CAUSAFAMILIA", "TEMPERATURA", "HUMIDADERELATIVA", "VENTOINTENSIDADE", "VENTOINTENSIDADE_VETOR",
               "VENTODIRECAO_VETOR", "PRECEPITACAO", "FFMC", "DMC", "DC", "ISI", "BUI", "FWI", "DSR", "THC", "MODFARSITE",
               "ALTITUDEMEDIA", "DECLIVEMEDIO", "HORASEXPOSICAOMEDIA", "DENDIDADERV", "COSN5VARIEDADE", "AREAMANCHAMODFARSITE",
               "AREASFICHEIROS_GNR", "AREASFICHEIROS_GTF", "FICHEIROIMAGEM_GNR", "AREASFICHEIROSHP_GTF", "AREASFICHEIROSHPXML_GTF",
               "AREASFICHEIRODBF_GTF", "AREASFICHEIROPRJ_GTF", "AREASFICHEIROSBN_GTF", "AREASFICHEIROSBX_GTF", "AREASFICHEIROSHX_GTF",
               "AREASFICHEIROZIP_SAA"]


def parse_XML(xml_file, df_cols):
    """source: https://medium.com/@robertopreste/from-xml-to-pandas-dataframes-9292980b1c1c"""
    xtree = ET.parse(xml_file)
    xroot = xtree.getroot()
    rows = []
    for node in xroot:
        res = []
        res.append(node.attrib.get(df_cols[0]))
        for el in df_cols[1:]:
            if node is not None and node.find(el) is not None:
                res.append(node.find(el).text)
            else:
                res.append(None)
        rows.append({df_cols[i]: res[i]
                     for i, _ in enumerate(df_cols)})
    out_df = pd.DataFrame(rows, columns=df_cols)
    return out_df


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
        df = parse_XML(io.StringIO(resp.text), ALL_COLUMNS)
        df[IDENTIFIER_COLUMNS[0]] = df[IDENTIFIER_COLUMNS[0]].astype(str)
        df = df.reset_index()
        print("Retrieved " + str(len(df)) + " entries for " + str(year) + "-" + str(month) + "-" + str(day))
        return df.replace("|", "/")
    else:
        print('No data for '+str(year) + "-" + str(month) + "-" + str(day))
        return pd.DataFrame()


def concat(df1, df2, year=None, month=None):
    if len(df1) == 0:
        return df2
    if len(df2) == 0:
        return df1
    merged_df = pd.concat([df1, df2]).drop_duplicates(subset=IDENTIFIER_COLUMNS, keep='first')
    df_sorted = merged_df.sort_values(IDENTIFIER_COLUMNS)
    df_sorted.reset_index()
    print('Merged '+str(year)+'-'+str(month)+': '+str(len(df1))+' + '+str(len(df2))+'='+str(len(df_sorted)))
    return df_sorted


def save_year_2_file(df, year, lock):
    filename = "data/" + str(year) + ".csv"
    if 'index' in df.columns:
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
        df[IDENTIFIER_COLUMNS[0]] = df[IDENTIFIER_COLUMNS[0]].astype(str)
        print("Read " + str(len(df)) + " entries from " + filename)
        lock.release()
        return df
    else:
        lock.release()
        return None


def retrieve_year(year, lockFile):
    try:
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
        save_year_2_file(accumulated_df, year, lockFile)
        print("Retrieved a total of " + str(len(accumulated_df)) + " for year " + str(year))
    except Exception as e:
        print("Error: unable retrieve year "+str(year))
        print(e)
        traceback.print_exception(e)

def retrieve_month(year, month, lockFile):
    try:
        df = retrieve(year, month, None)
        year_df = read_year_from_file(year, lockFile)
        if year_df is not None:
            year_df = concat(df, year_df, year, month)
            save_year_2_file(year_df, year, lockFile)
        else:
            save_year_2_file(df, year, lockFile)
    except Exception as e:
        print("Error: unable retrieve month "+str(year)+"-"+str(month))
        print(e)
        traceback.print_exception(e)

def retrieve_months(year, months, lockFile):
    print('Will retrieve the following months for year '+str(year)+': '+str(months))
    for month in months:
        retrieve_month(year, month, lockFile)


def retrieve_day(day, month, year, lockFile):
    df = retrieve(year, month, day)
    year_df = read_year_from_file(year, lockFile)
    if year_df is not None:
        year_df = concat(df, year_df)
        save_year_2_file(year_df, year, lockFile)
    else:
        save_year_2_file(df, year, lockFile)


def retrieve_last_days(ndays=10):
    print('Start retrieve last ' + str(ndays) + ' days')
    today = datetime.datetime.today()
    fromday = today - datetime.timedelta(days=ndays)
    lockFile = threading.Lock()
    if (today - fromday).days > 10:
        with ThreadPoolExecutor(max_workers=6) as executor:
            job_date = date(fromday.year, fromday.month, fromday.day)
            begin_of_month = date(today.year, today.month, 1)
            jobs = {}
            years = []
            while job_date <= begin_of_month:
                if str(job_date.year) not in jobs:
                    years.append(job_date.year)
                    jobs[str(job_date.year)] = []
                jobs[str(job_date.year)].append(job_date.month)
                if (job_date.month < 12):
                    job_date = date(job_date.year, job_date.month + 1, 1)
                else:
                    job_date = date(job_date.year + 1, 1, 1)
            try:
                futures = []
                print('We have the following jobs: '+str(jobs))
                for year in years:
                    futures.append(executor.submit(retrieve_months, year, jobs[str(year)], lockFile))

                complete_count = 0
                for future in concurrent.futures.as_completed(futures):
                    complete_count += 1
                    print('Completed tasks: ' + str(complete_count) + ' of ' + str(len(years)))
            except Exception as e:
                print("Error: unable to start thread")
                print(e)
                traceback.print_exception(e)
    else:
        for n in range(1, ndays + 1):
            dt = today - datetime.timedelta(days=n)
            year = dt.year
            month = dt.month
            day = dt.day
            retrieve_day(day, month, year, lockFile)
    print('End retrieve last ' + str(ndays) + ' days')


def retrieve_all():
    print('Start retrieve all')
    with ThreadPoolExecutor(max_workers=6) as executor:
        dt = datetime.datetime.today()
        current_year = dt.year
        jobs = range(2001, current_year + 1)
        lockFile = threading.Lock()
        try:
            futures = []
            print('We have the following jobs: '+str(jobs))
            for year in jobs:
                futures.append(executor.submit(retrieve_year, year, lockFile))

            complete_count = 0
            for future in concurrent.futures.as_completed(futures):
                complete_count += 1
                print('Completed tasks: ' + str(complete_count) + ' of ' + str(len(jobs)))
        except Exception as e:
            print("Error: unable to start thread")
            print(e)
            traceback.print_exception(e)
        print('End')


if sys.argv[1] == 'ndays':
    retrieve_last_days(int(sys.argv[2]))
elif sys.argv[1] == 'all':
    retrieve_all()
else:
    retrieve_year(int(sys.argv[1]), threading.Lock())
