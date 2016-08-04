"""
This plugin does not define any new operators, hooks etc. Instead, it exists to
install raven logging into Airflow.

Be sure that the SENTRY_DSN environment variable is set to the DSN for your
project on Sentry.
"""

import logging
from raven.handlers.logging import SentryHandler
from raven.conf import setup_logging

handler = SentryHandler(level=logging.WARNING)
setup_logging(handler)
