FROM ubuntu:16.04

# Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

# Airflow
ARG AIRFLOW_VERSION=1.7.1.3
ENV AIRFLOW_HOME /usr/local/airflow

# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8
ENV LC_ALL  en_US.UTF-8

RUN set -ex \
    && buildDeps=' \
        python-dev \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        build-essential \
        libblas-dev \
        liblapack-dev \
        libpq-dev \
    ' \
    && apt-get update -yqq \
    && apt-get install -yqq --no-install-recommends \
        $buildDeps \
        python-pip \
        apt-utils \
        curl \
        netcat \
        locales \
        git \
        wget \
        alien \
        libgdal-dev \
        libgeos-dev \
        binutils \
        libproj-dev \
        gdal-bin \
        libspatialindex-dev \
        libaio1 \
    && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    && useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow \
    && python -m pip install -U pip \
    && pip install -U setuptools \
    && pip install Cython \
    && pip install pytz==2015.7 \
    && pip install pyOpenSSL \
    && pip install ndg-httpsclient \
    && pip install pyasn1 \
    && pip install airflow[crypto,postgres,hive]==$AIRFLOW_VERSION \
    && apt-get remove --purge -yqq $buildDeps \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base

# instant sql-plus instant oracle client
RUN set -ex \
    && wget https://www.dropbox.com/s/ubgeht3m59bhfh1/oracle-instantclient12.1-sqlplus-12.1.0.2.0-1.x86_64.rpm?dl=0 \
    && mv oracle-instantclient12.1-sqlplus-12.1.0.2.0-1.x86_64.rpm?dl=0 oracle-instantclient12.1-sqlplus-12.1.0.2.0-1.x86_64.rpm \
    && alien -i oracle-instantclient12.1-sqlplus-12.1.0.2.0-1.x86_64.rpm \
    && rm oracle-instantclient12.1-sqlplus-12.1.0.2.0-1.x86_64.rpm

# instant basic-lite instant oracle client
RUN set -ex \
    && wget https://www.dropbox.com/s/1yzl0fdnaiw5yqp/oracle-instantclient12.1-basiclite-12.1.0.2.0-1.x86_64.rpm?dl=0 \
    && mv oracle-instantclient12.1-basiclite-12.1.0.2.0-1.x86_64.rpm?dl=0 oracle-instantclient12.1-basiclite-12.1.0.2.0-1.x86_64.rpm \
    && alien -i oracle-instantclient12.1-basiclite-12.1.0.2.0-1.x86_64.rpm \
    && rm oracle-instantclient12.1-basiclite-12.1.0.2.0-1.x86_64.rpm

# instant oracle-sdk
RUN set -ex \
    && wget https://www.dropbox.com/s/uic5vzc9yobttct/oracle-instantclient12.1-devel-12.1.0.2.0-1.x86_64.rpm?dl=0 \
    && mv oracle-instantclient12.1-devel-12.1.0.2.0-1.x86_64.rpm?dl=0 oracle-instantclient12.1-devel-12.1.0.2.0-1.x86_64.rpm \
    && alien -i oracle-instantclient12.1-devel-12.1.0.2.0-1.x86_64.rpm \
    && rm oracle-instantclient12.1-devel-12.1.0.2.0-1.x86_64.rpm

COPY scripts/entrypoint.sh /entrypoint.sh
COPY requirements.txt /requirements.txt

COPY config/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg
COPY dags ${AIRFLOW_HOME}/dags
COPY plugins ${AIRFLOW_HOME}/plugins

RUN chown -R airflow: ${AIRFLOW_HOME}

EXPOSE 8080 5555 8793

USER airflow
WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["/entrypoint.sh"]
