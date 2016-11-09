# phila-airflow

Airflow Configuration for The City of Philadelphia

## Local setup

```bash
# clone the repository
git clone https://github.com/CityOfPhiladelphia/phila-airflow.git
cd phila-airflow

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

### Troubleshooting
Several DAGs (pipelines) depend on python packages that depend on system
packages. For instance, [datum](https://github.com/CityOfPhiladelphia/datum)
depends on Oracle. If these system packages are not available,
`pip install -r requirements.txt` will likely fail. These issues are
DAG-specific but will be documented in this section to facilitate easier
troubleshooting.

#### Installing Oracle on OSX
Following [this guide](https://web.archive.org/web/20160407232743/http://kevindalias.com/2014/03/26/how-to-set-up-cx_oracle-for-python-on-mac-os-x-10-89):

1. Install the Oracle Instant Client. Download the 64-bit versions of the basic and sdk zip files [from oracle](http://www.oracle.com/technetwork/topics/intel-macsoft-096467.html).
2. Create a global oracle directory in a location such as `~/.local/share/oracle` and copy the two `.zip` files into it
3. Unzip the `.zip` files into that directory. When finished, the `oracle` directory should contain a bunch of files in it (rather than containing a single directory of files).
4. Inside the `oracle` directory, create symbolic links using:

```bash
ln -s libclntsh.dylib.11.1 libclntsh.dylib
ln -s libocci.dylib.11.1 libocci.dylib
```

Finally, add the following environment variables to your `~/.bash_profile`, replacing the value of `ORACLE_HOME` with the absolute path to your new `oracle` directory.

```bash
export ORACLE_HOME="/path/to/oracle"
export DYLD_LIBRARY_PATH=$ORACLE_HOME
export LD_LIBRARY_PATH=$ORACLE_HOME
```
Note: Alternatively, you can install the Oracle Instant Client inside your virtual environment directory and set the environment variables contextually for the specific project.

#### rtree
```
Command "python setup.py egg_info" failed with error code 1 in /private/.../rtree
```
The taxi trips workflow uses `rtree`, which depends on system packages that do not ship with OS X natively. To install them, use:
```bash
brew install spatialindex
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

### Creating your transformation script

Portions of a DAG can exist in code outside of the phila-airflow repository. For
example, you may want to create transformation scripts that can be plugged in
to Airflow that get installed when the server is deployed. Creating these as
separate scripts can make them easier to test.

Two handy libraries for writing transformation scripts are:
* [petl](https://petl.readthedocs.io/en/latest/)
* [click](http://click.pocoo.org/)

Also, to gain insight into any exceptions as they occur, install and list the
[raven](https://docs.sentry.io/hosted/clients/python/) library as a dependency
for your script.

```python
import click
import petl
import raven


# NOTE: The following function is just a sample command. Neither the name of
# the function nor the contents are special. They are simply for demonstration.
@click.command()
def cmd():
    t = petl.fromcsv('...')\
        .addfield('foo', 1)\
        .addfield('bar', lambda r: r[foo] + 1)
        .cutout('baz')\
        .tocsv()


if __name__ == '__main__':
    client = raven.Client()
    try:
        cmd()  # Call the function defined above.
    except:
        client.captureException()
        raise
```



You can reuse your own python modules in pipelines. In order to use a module,
it must be installable -- e.g., it should have a *setup.py* file. A minimal
*setup.py* file looks something like:

```python
from distutils.core import setup

setup(
    name='phl-data-transformation',
    version='1.0.0',
    scripts=['transform.py'],
)
```

### Defining the DAG

You should think of your pipeline as a series of stages.

```python

# constructing a new dag requires importing airflow’s DAG class:
from airflow import DAG

# use the DAG class to instantiate a new dag, provide arguments for the name, and default settings.
# The latter can be created with a dictionary and will be applied to all of your operators:

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

## Troubleshooting Plugins

Sometimes Airflow will fail to import components that you've made available via plugins. In order to troubleshoot your plugin, follow these instructions:

1. Open a Python REPL and import Airflow

   ```python
   >>> from airflow import *
   ```

   If Python gives you any exceptions, you may have a syntax error in one of your modules.

2. Import the components that the plugin provides. For example, if your plugin exposes a hook, import it from `airflow.hooks`; if it exposes an operator import it from `airflow.operators`.
