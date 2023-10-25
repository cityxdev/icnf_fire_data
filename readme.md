# ICNF Fire Data Retriever

This project aims to provide a way of downloading and structuring data about wildfires in Portugal, in a manner that is easy to use.
It downloads data from ICNF webservice at https://fogos.icnf.pt/localizador/webserviceocorrencias.asp and inserts in a PostgreSQL/PostGIS db.<br/>
Main inspiration: [VOSTPT](https://github.com/vostpt/ICNF_DATA)

## Pre-requisites
* This was created to work on Linux (some adjustments must be done of other OS)
* Python3 (libs: pandas, psycopg2)
* PostgreSQL and PostGIS
* GDAL

## Database connection
Both `build.sh` and `filler.py` assume you have a PostgreSQL database (named e.g. `icnf_fire_data`) and a `db.ini` file with the following content:
```
[DEFAULT]
host = HOST
port = PORT (default 5432)
dbname = DB_NAME
user = USERNAME
password = PASSWORD
```

## Build
This is the way to build what this project needs to run on your machine:<br/>
`./build.sh`<br/>

## Retrieve data
`retriever.py` will download data from the webservice and transform it into CSV files (separator is |), one per year.<br/>
There is data from 2001.<br/>
If you just need old data in CSV, you do not need to run anything, just use the files under /data (the one for the current year is not updated)
* Retrieve since 2001: `python3 retriever.py all`
* Retrieve a certain year (e.g. 2017): `python3 retriever.py 2017`
* Retrieve last x days (e.g. 10): `python3 retriever.py ndays 10`

## From CSV to PostgreSQL
`filler.py` reads the files under `data/` and writes the data to the database.
* Write everything in `data/` to the db: `python3 filler.py`
* Write the last x days in `data/` to the db (e.g. 10 days): `python3 filler.py ndays 10`

## Polygons
`polygons_fetcher.py` fetches geographical data from files referenced by the data and creates a multipolygon for each fire (that has referenced accessible files).<br/>
This is an **experimental** feature that lacks reliability because many links are dead and there seems to be no classification of the actual files.
* Fetch polygons for the whole data set (this can take a lot of time to run): `python3 polygons_fetcher.py`
* Fetch polygons for fires in the last x days (e.g. 10 days): `python3 polygons_fetcher.py ndays 10`


## Update utilities
There are utilities to update data regularly, in order to get new data and also update recent entries:
* `update_daily.sh` and `update_daily_with_polygons.sh` is meant to be run daily and overwrites data from the last 5 days 
* `update_weekly.sh` and `update_weekly_with_polygons.sh` is meant to be run weekly and overwrites data from the last 30 days 
* `update_monthly.sh` and `update_monthly_with_polygons.sh` is meant to be run monthly and overwrites data from the last 730 days 



## Examples

### Build from scratch (~30min)
1. Create a db.ini file
2. `createdb DB_NAME ...`
3. `python3 retriever.py all`
4. `./build.sh`

### crontab for daily updates at 1 am
`0 1 * * * /path/to/project/update_daily.sh`

### crontab for weekly updates every sunday at 1 am
`0 1 * * SUN /path/to/project/update_weekly.sh`

### crontab for monthly updates day 1 at 1 am
`0 1 1 * * /path/to/project/update_monthly.sh`



## Docker
If you do not have an accessible PostgreSQL server and do not wish to install it in your machine, you can
use a docker container to run it.
```
cd docker
./build.sh  #might need to use sudo
```
or
```
cd docker
./build.sh --with-polygons #might need to use sudo
```
This will start a container on your machine with a PostgreSQL db containing the ICNF fire data.<br/>
You can check db connection params in `docker/db.ini`<br/>
`./build.sh --with-polygons` will take a lot of time to run!<br/>
After build, you can start and stop your container with:<br/>
`docker container start icnf_fire_data_container` <br/>
`docker container stop icnf_fire_data_container`

You can also ssh to the running container with the following:<br/>
`ssh-keygen -f ~/.ssh/known_hosts -R "[localhost]:8022" ; ssh fire@localhost -p8022` <br/>
password: `fire_pw`
