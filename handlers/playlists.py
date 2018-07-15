import logging
from io import StringIO

import sh

logger = logging.getLogger(__name__)


def handle(payload):
    buf = StringIO()
    err_buf = StringIO()

    sh.luigi("--module", "tasks.playlists",
             "vtalks.playlists.Playlist", "--workers", "5", "--youtube-url", payload,
             _out=buf,
             _err=err_buf)
