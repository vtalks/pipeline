import logging
from io import StringIO

import sh

logger = logging.getLogger("handlers-talk")


def handle(payload):
    buf = StringIO()
    err_buf = StringIO()

    sh.luigi("--module", "tasks.talks",
             "vtalks.talks.Talk", "--youtube-url", payload,
             _out=buf,
             _err=err_buf)

    logger.setLevel(logging.INFO)

    logging.info(buf.getvalue())
    logging.error(err_buf.getvalue())

    logger.setLevel(logging.WARNING)
