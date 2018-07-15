import logging
from io import StringIO

import sh

logger = logging.getLogger("handlers-playlist")


def handle(payload):
    buf = StringIO()
    err_buf = StringIO()

    sh.luigi("--module", "tasks.playlists",
             "vtalks.playlists.Playlist", "--youtube-url", payload,
             _out=buf,
             _err=err_buf)

    logging.info(buf.getvalue())
    logging.error(err_buf.getvalue())
