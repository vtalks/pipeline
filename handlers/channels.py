import logging
from io import StringIO

import sh

logger = logging.getLogger("handlers-channel")


def handle(payload):
    buf = StringIO()
    err_buf = StringIO()

    sh.luigi("--module", "tasks.channels",
             "vtalks.channels.Channel", "--youtube-url", payload,
             _out=buf,
             _err=err_buf)

    logging.info(buf.getvalue())
    logging.error(err_buf.getvalue())
