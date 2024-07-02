import shutil
import sys
import configparser
import datetime
import zipfile
from datetime import date
import urllib.request
import urllib.parse
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import psycopg2
import os
from os.path import exists
import glob

config = configparser.ConfigParser()
config.read("db.ini")
dbname = config.get("DEFAULT", "dbname")
user = config.get("DEFAULT", "user")
host = config.get("DEFAULT", "host")
port = "5432" if not config.has_option("DEFAULT", "port") else config.get("DEFAULT", "port")
password = config.get("DEFAULT", "password")
connect_str = "host=" + host + " port=" + port + " dbname=" + dbname + " user=" + user + " password=" + password


def fetch_polygons_for_file_list(id, file_list):
    print('Start fetch polygons for fire ' + str(id))
    directory = str(id) + "_files/"
    if exists(directory):
        shutil.rmtree(directory)
    os.mkdir(directory)
    has_error = False
    conn = None
    try:
        for url in file_list:
            if url is not None and len(url.strip()) > 1:
                filename = url.split('/')[-1]
                fixed_filename = urllib.parse.quote(filename, safe='')
                fixed_url = url.replace(filename, fixed_filename)
                try:
                    urllib.request.urlretrieve(fixed_url, directory + fixed_filename)
                    if fixed_filename.endswith(".zip"):
                        with zipfile.ZipFile(directory+fixed_filename, "r") as zip_ref:
                            zip_ref.extractall(directory)
                except Exception as e:
                    print("Error on fire id "+str(id)+" downloading URL "+fixed_url)
                    print(e)

        tablename = "file_"+str(id)
        conn = psycopg2.connect(connect_str)
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS "+tablename)
        conn.commit()
        has_data = False
        for ext in [".kml", ".kmz", ".shp"]:
            for fname in glob.glob(directory+"**/*"+ext, recursive=True):
                command = '''ogr2ogr -lco GEOMETRY_NAME=geom -f "PostgreSQL" PG:"host=''' + host + ''' port=''' + port + ''' user=''' + user + ''' password=''' + password + ''' dbname=''' + dbname + '''" '''+fname+''' -nln '''+tablename
                try:
                    os.system(command)
                    has_data = True
                except Exception as e:
                    print("Error on fire id "+str(id)+" processing file "+fname)
                    print(command)
                    has_error = True
                    print(e)
        if has_data:
            update_command = "UPDATE fire" \
                             " SET multipolygon = (SELECT ST_Transform(ST_MakeValid(ST_Multi(ST_CollectionExtract(ST_Union(ST_MakeValid(ST_SnapToGrid(geom,0.00001))),3))),4326)::geography FROM "+tablename+") WHERE id=%s;"
            update_command = update_command + "UPDATE fire SET multipolygon = NULL WHERE id=%s AND (ST_IsEmpty(multipolygon::geometry) OR not(ST_IsValid(multipolygon::geometry)));"
            try:
                cursor.execute(update_command, [id, id])
                conn.commit()
                cursor.execute("DROP TABLE IF EXISTS "+tablename)
                conn.commit()
            except Exception as e:
                print("Error on fire id "+str(id)+" processing data in table "+tablename)
                print(update_command.replace("%s", str(id)))
                has_error = True
                print(e)
    except Exception as e:
        print("Unexpected error on fire id "+str(id))
        print(e)
    finally:
        if not has_error:
            shutil.rmtree(directory)
        if conn is not None:
            conn.close()
    print('End fetch polygons for fire ' + str(id))


def fetch_polygons(year, month, day):
    print('Start fetch polygons ' + str(year)+"-"+str(month)+"-"+str(day))
    date_from = date(year, month if month is not None else 1, day if day is not None else 1)
    conn = psycopg2.connect(connect_str)
    cursor = conn.cursor()
    cursor.execute("SELECT id, file_urls FROM fire WHERE year = %s AND ts >= %s ORDER BY ts DESC", [year, date_from])
    results = cursor.fetchall()
    for row in results:
        if row[1] is not None and len(row[1]) > 0:
            fetch_polygons_for_file_list(row[0], row[1].split(','))
    print('End fetch polygons ' + str(year)+"-"+str(month)+"-"+str(day))


def fetch_polygons_from(fromyear, frommonth, fromday):
    with ThreadPoolExecutor(max_workers=6) as executor:
        print('Start fetch polygons from ' + str(fromyear) + "-" + str(frommonth) + "-" + str(fromday))
        dt = datetime.datetime.today()
        jobs = range(fromyear if fromyear is not None else 2001, dt.year + 1)
        try:
            futures = []
            for year in jobs:
                if year == fromyear:
                    futures.append(executor.submit(fetch_polygons, year, frommonth, fromday))
                else:
                    futures.append(executor.submit(fetch_polygons, year, None, None))

            complete_count = 0
            for future in concurrent.futures.as_completed(futures):
                complete_count += 1
                print('Completed tasks: ' + str(complete_count) + ' of ' + str(len(jobs)))

        except Exception as e:
            print("Error: unable to start thread")
            print(e)
    print('End fetch polygons from ' + str(fromyear) + "-" + str(frommonth) + "-" + str(fromday))

if len(sys.argv) == 4:
    year = int(sys.argv[1])
    month = int(sys.argv[2])
    day = int(sys.argv[3])
    fetch_polygons(year, month, day)
elif len(sys.argv) == 3 and sys.argv[1] == 'ndays':
    dt = datetime.datetime.today() - datetime.timedelta(days=int(sys.argv[2]))
    year = dt.year
    month = dt.month
    day = dt.day
    fetch_polygons_from(year, month, day)
else:
    fetch_polygons_from(None, None, None)
