import configparser
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

cursor.execute(open("db/build.sql", "r").read())
conn.commit()

cursor.execute(open("db/process_lau.sql", "r").read())
conn.commit()

cursor.execute(open("db/create_layers.sql", "r").read())
conn.commit()

cursor.execute(open("db/create_dashboard_views.sql", "r").read())
conn.commit()

conn.close()
