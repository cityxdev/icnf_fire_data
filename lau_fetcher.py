import configparser
import os
import shutil
import urllib.request
import zipfile
import psycopg2

config = configparser.ConfigParser()

config.read("db.ini")
dbname = config.get("DEFAULT", "dbname")
user = config.get("DEFAULT", "user")
host = config.get("DEFAULT", "host")
port = "5432" if not config.has_option("DEFAULT", "port") else config.get("DEFAULT", "port")
password = config.get("DEFAULT", "password")

connect_str = "host=" + host + " port=" + port + " dbname=" + dbname + " user=" + user + " password=" + password
conn = psycopg2.connect(connect_str)
cursor = conn.cursor()
cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
conn.commit()
conn.close()

config.read("lau.ini")
url = config.get("DEFAULT", "url")

urllib.request.urlretrieve(url, "lau.zip")
with zipfile.ZipFile("lau.zip", "r") as zip_ref:
    zip_ref.extractall("lau")
    os.remove("lau.zip")
    command = '''ogr2ogr -f "PostgreSQL" PG:"host=''' + host + ''' port=''' + port + ''' user=''' + user + ''' password=''' + password + ''' dbname=''' + dbname + '''" lau/Continente/Cont_AAD_CAOP*.shp -nln lau'''
    os.system(command)
    shutil.rmtree("lau")
