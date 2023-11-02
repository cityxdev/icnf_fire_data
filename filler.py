import concurrent.futures
import configparser
import csv
import datetime
import sys
from concurrent.futures import ThreadPoolExecutor
from os.path import exists
import psycopg2

config = configparser.ConfigParser()
config.read("db.ini")
dbname = config.get("DEFAULT", "dbname")
user = config.get("DEFAULT", "user")
host = config.get("DEFAULT", "host")
port = "5432" if not config.has_option("DEFAULT", "port") else config.get("DEFAULT", "port")
password = config.get("DEFAULT", "password")
connect_str = "host=" + host + " port=" + port + " dbname=" + dbname + " user=" + user + " password=" + password


def fill(year, month, day):
    print('Start fill ' + str(year)+"-"+str(month)+"-"+str(day))
    filename = "data/" + str(year) + ".csv"
    if exists(filename):
        with open(filename, mode='r') as f:
            try:
                conn = psycopg2.connect(connect_str)
                cursor = conn.cursor()
                datareader = csv.reader(f, delimiter='|')
                col_names = next(datareader)
                col_names[0] = 'original_id'
                col_names_str = ", ".join(col_names)
                param_placeholders = ["%s"] * len(col_names)
                param_placeholders_str = ", ".join(param_placeholders)
                command = "INSERT INTO raw.data (" + col_names_str + ") VALUES (" + param_placeholders_str + ");"
                for row in datareader:
                    rowmonth = int(row[col_names.index("MES")])
                    rowday = int(row[col_names.index("DIA")])
                    if ((month is None or rowmonth >= month) and (day is None or rowmonth > month or rowday >= day)):
                        params = [None if x == '' else x for x in row]
                        cursor.execute(command, params)
                conn.commit()
                print("Delete duplicates from " + str(year))
                delete_command = "DELETE FROM raw.data\
                                    WHERE ano = %s AND ctid NOT IN (\
                                        SELECT max(ctid)\
                                        FROM raw.data\
                                        WHERE ano = %s\
                                        GROUP BY original_id)"
                cursor.execute(delete_command, [year, year])
                conn.commit()
                conn.close()
            except Exception as e:
                print("Error on " + str(year)+"-"+str(month)+"-"+str(day) + " (" + connect_str + ")")
                print(e)
    print('End fill ' + str(year)+"-"+str(month)+"-"+str(day))



def fill_from(fromyear, frommonth, fromday):
    with ThreadPoolExecutor(max_workers=6) as executor:
        delete_from(fromday, frommonth, fromyear)
        print('Start fill from ' + str(fromyear) + "-" + str(frommonth) + "-" + str(fromday))
        dt = datetime.datetime.today()
        jobs = range(fromyear if fromyear is not None else 2001, dt.year + 1)
        try:
            futures = []
            for year in jobs:
                if year == fromyear:
                    futures.append(executor.submit(fill, year, frommonth, fromday))
                else:
                    futures.append(executor.submit(fill, year, None, None))

            complete_count = 0
            for future in concurrent.futures.as_completed(futures):
                complete_count += 1
                print('Completed tasks: ' + str(complete_count) + ' of ' + str(len(jobs)))

            print("Start process new data")
            conn = psycopg2.connect(connect_str)
            cursor = conn.cursor()
            cursor.execute("SELECT process_raw_data(%s,%s,%s);", [fromyear, frommonth, fromday])
            conn.commit()
            conn.close()
            print("End process new data")
        except Exception as e:
            print("Error: unable to start thread")
            print(e)
    print('End fill from ' + str(fromyear) + "-" + str(frommonth) + "-" + str(fromday))


def delete_from(fromday, frommonth, fromyear):
    print('Start delete from ' + str(fromyear) + "-" + str(frommonth) + "-" + str(fromday))
    delete_command = "DELETE FROM raw.data"
    delete_params = []
    conn = psycopg2.connect(connect_str)
    cursor = conn.cursor()
    if (fromyear is not None):
        delete_command += " WHERE ano::integer>%s"
        delete_params.append(fromyear)
        if (frommonth is not None):
            delete_command += " AND mes::integer>%s"
            delete_params.append(frommonth)
            if (fromday is not None):
                delete_command += " AND dia::integer>%s"
                delete_params.append(fromday)
    cursor.execute(delete_command, delete_params)
    conn.commit()
    conn.close()
    print('End delete from ' + str(fromyear) + "-" + str(frommonth) + "-" + str(fromday))


if len(sys.argv) > 2 and sys.argv[1] == 'ndays':
    dt = datetime.datetime.today() - datetime.timedelta(days=int(sys.argv[2]))
    year = dt.year
    month = dt.month
    day = dt.day
    fill_from(year, month, day)
else:
    fill_from(None, None, None)

