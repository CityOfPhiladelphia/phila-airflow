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
from tempfile import mkstemp, NamedTemporaryFile


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
# S3

@contextmanager
def stream_from_s3(conn_id, path):
    from airflow.hooks.S3_hook import S3Hook
    conn = S3Hook(s3_conn_id=conn_id)

    s3_obj = conn.get_key(path)
    with NamedTemporaryFile() as fileob:
        s3_obj.get_contents_to_file(fileob)
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
    's3': stream_from_s3,
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
# S3

@contextmanager
def stream_to_s3(conn_id, path):
    from airflow.hooks.S3_hook import S3Hook
    conn = S3Hook(s3_conn_id=conn_id)

    with NamedTemporaryFile() as fileob:
        yield fileob

        conn.load_file(
            filename=fileob.name,
            key=path,
            replace=True
        )

# ---------------------------------------------------------
# Local file system

@contextmanager
def stream_to_local(conn_id, path):
    with open(path, 'wb') as fileob:
        yield fileob

# ---------------------------------------------------------

stream_to = {
    's3': stream_to_s3,
    'local': stream_to_local,
}

# =========================================================
# Operators

class FileTransferOperator(BaseOperator):
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
                 source_type,
                 dest_type,
                 source_path,
                 dest_path,
                 source_conn_id=None,
                 dest_conn_id=None,
                 *args, **kwargs):
        super(FileTransferOperator, self).__init__(*args, **kwargs)

        self.source_type = source_type
        self.source_conn_id = source_conn_id
        self.source_path = source_path

        self.stream_from = stream_from[source_type]

        self.dest_type = dest_type
        self.dest_conn_id = dest_conn_id
        self.dest_path = dest_path

        self.stream_to = stream_to[dest_type]

    def execute(self, context):
        with self.open_source() as source:
            with self.open_dest() as dest:
                self.transfer(source, dest)

    def open_source(self):
        logging.info("Opening file {} on {} source {}."
            .format(self.source_path, self.source_type, self.source_conn_id))
        return self.stream_from(self.source_conn_id, self.source_path)

    def open_dest(self):
        logging.info("Opening file {} on {} dest {}."
            .format(self.dest_path, self.dest_type, self.dest_conn_id))
        return self.stream_to(self.dest_conn_id, self.dest_path)

    def transfer(self, source, dest):
        logging.info("Transferring data from source to destination.")
        for chunk in source:
            dest.write(chunk)


class FileTransformOperator (FileTransferOperator):
    """
    Retrieve a file from a connection, transforms it by some executable script,
    and store it on another connection.

    :param source_conn_id: The connection to run the operator against.
    :type source_conn_id: string
    :param source_path: The path to the file on the server
    :type source_path: string
    :param dest_path: The full path to which the file will be downloaded
    :type dest_path: string
    """

    template_fields = ('source_path','dest_path','transform_script')

    @apply_defaults
    def __init__(self,
                 transform_script,
                 use_stdin=True,
                 use_stdout=True,
                 *args, **kwargs):
        super(FileTransformOperator, self).__init__(*args, **kwargs)
        self.transform_script = transform_script
        self.use_stdin = use_stdin
        self.use_stdout = use_stdout

    def transfer(self, source, dest):
        # Dump the source to a temporary file
        with NamedTemporaryFile() as f_source:
            logging.info('Dumping source data to a file: ' + f_source.name)
            f_source.write(source.read())
            f_source.seek(0)

            # Open a temporary file to receive the output from the
            # transformation script
            with NamedTemporaryFile() as f_dest:
                self.transform(f_source, f_dest)
                f_dest.seek(0)

                # Write the transformed data to the destination
                logging.info('Transferring transformed data from to destination.')
                for chunk in f_dest:
                    dest.write(chunk)

    def transform(self, f_source, f_dest):
        script_args = [self.transform_script]

        if not self.use_stdin:
            script_args.append(f_source.name)

        if not self.use_stdout:
            script_args.append(f_dest.name)

        logging.info('Running the transformation script command: ' +
                     ' '.join(script_args))
        self.sp = subprocess.run(script_args,
                                 stdin=f_source if self.use_stdin else None,
                                 stdout=f_dest if self.use_stdout else None,
                                 stderr=subprocess.PIPE)

        if self.sp.returncode > 0:
            raise AirflowException("Transform script failed " + self.sp.stderr)
        else:
            logging.info("Transform script successful. "
                         "Output temporarily located at {0}"
                         "".format(f_dest.name))


# =========================================================
# Plugin Definition

class FileTransferPlugin(AirflowPlugin):
    name = "file_transfer_plugin"
    operators = [FileTransferOperator, FileTransformOperator]
