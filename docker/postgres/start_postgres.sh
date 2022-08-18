#!/bin/bash

if [ -d "/home/fire/postgres_conf/" ]; then
	cp /home/fire/postgres_conf/*.conf /etc/postgresql/13/main/
  rm -r /home/fire/postgres_conf/
	service postgresql start 
	sudo -u postgres createdb icnf_fire_data 
	sudo -u postgres psql template1 -c "CREATE USER fire_read WITH PASSWORD 'fire_pw' ; GRANT CONNECT ON DATABASE icnf_fire_data TO fire_read ; ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO fire_read; "
	sudo -u postgres psql template1 -c "CREATE USER fire WITH SUPERUSER PASSWORD 'fire_pw' ; " 
else
	service postgresql start 	
fi
