# phl-airflow

Airflow Configuration for The City of Philadelphia

## Local setup

```bash
# clone the repository
git clone https://github.com/CityOfPhiladelphia/phl-airflow.git
cd phl-airflow

# airflow needs a home, ~/airflow is the default,
# but you can lay foundation somewhere else if you prefer
# (optional)
export AIRFLOW_HOME=`pwd`

# install the project requirements
pip install -r requirements.txt

# initialize the database
airflow initdb

# start the web server, default port is 8080
airflow webserver -p 8080
```
