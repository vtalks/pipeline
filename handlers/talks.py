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

    logging.getLogger().setLevel(logging.DEBUG)

    logging.info(buf.getvalue())
    logging.error(err_buf.getvalue())

    logging.getLogger().setLevel(logging.WARNING)
