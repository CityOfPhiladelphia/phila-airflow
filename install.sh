#!/bin/bash
# run to install airflowphl and all its dependencies

apt-get update

# install phila-airflow dependencies
apt-get install -y build-essential libssl-dev libffi-dev
apt-get install -y python3 python3-pip python3-setuptools python3-dev postgresql-client postgresql-client-common

# install redis tools
apt-get install -y redis-tools
apt-get install -y redis-server

# clone phila-airflow
apt-get install -y git alien wget libaio1

# grab instant sql-plus instant oracle client/ rename downloaded file and install with alien
wget https://www.dropbox.com/s/ubgeht3m59bhfh1/oracle-instantclient12.1-sqlplus-12.1.0.2.0-1.x86_64.rpm?dl=0
mv oracle-instantclient12.1-sqlplus-12.1.0.2.0-1.x86_64.rpm\?dl\=0 oracle-instantclient12.1-sqlplus-12.1.0.2.0-1.x86_64.rpm
alien -i oracle-instantclient12.1-sqlplus-12.1.0.2.0-1.x86_64.rpm

# grab instant basic-lite instant oracle client/ rename downloaded file and install with alien
wget https://www.dropbox.com/s/1yzl0fdnaiw5yqp/oracle-instantclient12.1-basiclite-12.1.0.2.0-1.x86_64.rpm?dl=0
mv oracle-instantclient12.1-basiclite-12.1.0.2.0-1.x86_64.rpm?dl=0 oracle-instantclient12.1-basiclite-12.1.0.2.0-1.x86_64.rpm
alien -i oracle-instantclient12.1-basiclite-12.1.0.2.0-1.x86_64.rpm

# grab instant oracle-sdk / rename downloaded files and install with alien
wget https://www.dropbox.com/s/uic5vzc9yobttct/oracle-instantclient12.1-devel-12.1.0.2.0-1.x86_64.rpm?dl=0
mv oracle-instantclient12.1-devel-12.1.0.2.0-1.x86_64.rpm?dl=0 oracle-instantclient12.1-devel-12.1.0.2.0-1.x86_64.rpm
alien -i oracle-instantclient12.1-devel-12.1.0.2.0-1.x86_64.rpm

# set oracle environment variables
export LD_LIBRARY_PATH /usr/lib/oracle/12.1/client64/lib/${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}
export ORACLE_HOME /usr/lib/oracle/12.1/client64

# geospatial dependencies
apt-get install -y libgdal-dev libgeos-dev binutils libproj-dev gdal-bin
apt-get install -y libspatialindex-dev

# install airflow dependencies
pip3 install -r ./requirements.txt

echo 'FINISHED'












































