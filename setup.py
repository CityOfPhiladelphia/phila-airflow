from setuptools import setup
from requirements import r

setup(
    name='phila-airflow',
    version='0.0.1',
    **r.dependencies)
