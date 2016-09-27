from airflow.plugins_manager import AirflowPlugin

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from past.builtins import basestring

import logging
log = logging.getLogger(__name__)

# =========================================================
# Operators

class CreateStagingFolder (BaseOperator):
    """
    Create a temporary folder for storing working files.

    If *dir* is not None, the file will be created in that directory;
    otherwise, a default directory is used. This behavior is according to the
    `mkstemp` function --
    https://docs.python.org/3/library/tempfile.html#tempfile.mkstemp
    """
    template_fields = ('dir',)
    ui_color = '#ffcc44'

    @apply_defaults
    def __init__(self, dir=None,
                 *args, **kwargs):
        super(CreateStagingFolder, self).__init__(*args, **kwargs)
        self.dir = dir

    def execute(self, context):
        from tempfile import mkdtemp
        log.debug('Creating staging folder' + ('in {}'.format(self.dir) if self.dir else ''))
        return mkdtemp(dir=self.dir)


class DestroyStagingFolder (BaseOperator):
    """
    Delete a folder created for storing working files.
    """
    template_fields = ('dir',)
    ui_color = '#ffcc44'

    @apply_defaults
    def __init__(self, dir,
                 *args, **kwargs):
        super(DestroyStagingFolder, self).__init__(*args, **kwargs)
        self.dir = dir

    def execute(self, context):
        from shutil import rmtree
        log.debug('Deleting the staging folder {}'.format(self.dir))
        rmtree(self.dir)


# =========================================================
# Plugin Definition

class StagingFolderPlugin(AirflowPlugin):
    name = "staging_folder_plugin"
    operators = [CreateStagingFolder, DestroyStagingFolder]
