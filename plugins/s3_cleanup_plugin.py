import logging
import subprocess

from airflow.plugins_manager import AirflowPlugin

from airflow.exceptions import AirflowException
from airflow.hooks import BaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from contextlib import contextmanager
from io import BytesIO
from os.path import exists
from tempfile import mkstemp


# =========================================================
# Source Context Managers
#
# Each source context manager should take a connection id (conn_id) and a path
# (path) that describes the location of a file to be read from the connection
# source. The context manager should yield a file-like object that is able to
# be streamed from.

# ---------------------------------------------------------
# FTP

@contextmanager
def stream_from_ftp(conn_id, path, secure=False):
    from airflow.contrib.hooks.ftp_hook import FTPHook, FTPSHook
    Hook = (FTPSHook if secure else FTPHook)
    conn = Hook(ftp_conn_id=conn_id)

    fileob = BytesIO()

    conn.retrieve_file(path, fileob)
    yield fileob

# ---------------------------------------------------------
# SFTP

@contextmanager
def stream_from_sftp(conn_id, path):
    with stream_from_ftp(conn_id, path, secure=True) as fileob:
        yield fileob

# ---------------------------------------------------------
# Local file system

@contextmanager
def stream_from_local(conn_id, path):
    with open(path, 'rb') as fileob:
        yield fileob

# ---------------------------------------------------------

stream_from = {
    'ftp': stream_from_ftp,
    'sftp': stream_from_sftp,
    'local': stream_from_local,
}

# =========================================================
# Destination Context Managers
#
# Each destination context manager should take a connection id (conn_id) and a
# path (path) that describes the location of a file to be written to in the
# connection. The context manager should yield a file-like object that is able
# to be streamed to.

# ---------------------------------------------------------
# Local file system

@contextmanager
def stream_to_local(conn_id, path):
    with open(path, 'wb') as fileob:
        yield fileob

# ---------------------------------------------------------

stream_to = {
    'local': stream_to_local,
}

# =========================================================
# Operators

class S3CleanupOperator(BaseOperator):
    """
    Downloads a file from a connection.

    :param source_conn_id: The connection to run the operator against.
    :type source_conn_id: string
    :param source_path: The path to the file on the server
    :type source_path: string
    :param dest_path: The full path to which the file will be downloaded
    :type dest_path: string
    :param secure: Whether to use a secure connection
    :type secure: bool
    """

    template_fields = ('source_path','dest_path',)
    template_ext = ()
    ui_color = '#f4a460'

    @apply_defaults
    def __init__(self,
                 s3_conn_id,
                 s3_key,
                 *args, **kwargs):
        super(S3CleanupOperator, self).__init__(*args, **kwargs)

        self.s3_conn_id = s3_conn_id
        self.s3_key = s3_key

    def execute(self, context):
        from airflow.contrib.hooks.ftp_hook import FTPHook, FTPSHook
        conn = S3Hook(s3_conn_id=self.s3_conn_id)
        
# =========================================================
# Plugin Definition

class S3CleanupPlugin(AirflowPlugin):
    name = "s3_cleanup_plugin"
    operators = [S3CleanupOperator]
