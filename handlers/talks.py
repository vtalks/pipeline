import logging
from io import StringIO

import sh

logger = logging.getLogger("handlers-talk")


def handle(payload):
    buf = StringIO()
    err_buf = StringIO()

    sh.luigi("--module", "tasks.talks",
             "vtalks.talks.Talk", "--youtube-url", payload,
             _long_prefix="--",
             _long_sep=" ",
             _out=buf,
             _err=err_buf)

    logger.info(buf.getvalue())
    logger.error(err_buf.getvalue())
