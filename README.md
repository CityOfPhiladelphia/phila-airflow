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

## Encryption

You can store connection credentials and other sensitive information in your
database, but you'll want to store it encrypted. Open your *airflow.cfg* file
and find the `fernet_key` line. In a terminal, run:

```bash
scripts/generate_enc_key
```

Use the output of that command as the `fernet_key` value.


## Dag Construction

```python

# constructing a new dag requires importing airflow’s DAG class:
from airflow import DAG

# use the DAG class to instantiate a new dag, provide arguments for the name, and default settings. The latter can be created with a dictionary:

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': seven_days_ago,
    'email': ['airflow@airflow.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
 }

dag = DAG(‘dag_name’, default_args=default_args)

# create the individual tasks you would like to run:
# run ChangeProjections.py

task1 =  BashOperator(
          task_id ='batch_projections',
          bash_command='''python  ChangeProjections.py ''',
          dag=dag
        )

# run Compare_privileges.py

task2 = BashOperator(
          task_id ='compare_priv',
          bash_command='''python Compare_privileges.py ''',
          dag=dag
        )

# run knack.py

  task3 = BashOperator(
          task_id='back_up_knack',
          bash_command='''python knack.py ''',
          dag=dag
  )


success = 'Your Task finished without errors'

task4 = SlackAPIPostOperator(
   task_id='notfiy_success',
   token=Variable.get('slack_token'),
   channel='yourchannel',
   text=success,
   dag=dag
)

task2.execute()

# to set the dependency of one task on another use the convention:
task1 >> task4
task2 >> task4
task3 >> task4

```

## Troubleshooting Plugins

Sometimes Airflow will fail to import components that you've made available via plugins. In order to troubleshoot your plugin, follow these instructions:

1. Open a Python REPL and import Airflow

   ```python
   >>> from airflow import *
   ```

   If Python gives you any exceptions, you may have a syntax error in one of your modules.

2. Import the components that the plugin provides. For example, if your plugin exposes a hook, import it from `airflow.hooks`; if it exposes an operator import it from `airflow.operators`.
