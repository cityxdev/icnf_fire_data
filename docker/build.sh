#!/bin/bash

echo '=========================Lets stop any running stuff========================='
docker container stop icnf_fire_data_container ; docker container rm icnf_fire_data_container ; docker image rm icnf_fire_data

echo '=========================Lets build the container and launch it========================='
docker build -t icnf_fire_data .
docker run -d -p 8022:22 -p 5434:5432 --name icnf_fire_data_container icnf_fire_data

echo '=========================Lets update data========================='
mv ../db.ini ../db.ini.bak
cp db.ini ../db.ini

cd ..
apt install python3 -y && apt install python3-pip -y && pip3 install pandas && pip3 install psycopg2-binary
if [ "$1" == "--with-polygons" ]; then
	./build_with_polygons.sh
else
	./build.sh
fi
cd docker

echo '=========================Lets cleanup========================='
mv ../db.ini.bak ../db.ini

echo '=========================END========================='

