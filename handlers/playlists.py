import logging
from io import StringIO

import sh

logger = logging.getLogger(__name__)


def handle(payload):
    buf = StringIO()
    err_buf = StringIO()

    sh.luigi("--module", "tasks.playlists",
             "vtalks.playlists.Playlist", "--youtube-url", payload,
             _out=buf,
             _err=err_buf)

    logger.debug(buf.getvalue())
    logger.error(err_buf.getvalue())

