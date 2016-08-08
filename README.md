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

## Dag Construction

```python

# constructing a new dag requires importing airflow’s DAG class:
from airflow import DAG

# use the DAG class to instantiate a new dag, provide arguments for the name, and default settings.\
#The latter can be created with a dictionary and will be applied to all of your operators:

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
# set task4 downstream of task1
task1 >> task4

# set task 4 downstream of task2
task2 >> task4

# set task4 downstream of task3
task3 >> task4

```
