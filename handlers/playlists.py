import asyncio
import logging

import luigi

from tasks.playlists import playlist

logger = logging.getLogger(__name__)


@asyncio.coroutine
async def pipeline_playlist_message_handler(msg):
    """ Playlist message handler for the data pipeline scheduler
    """
    subject = msg.subject
    reply = msg.reply
    payload = msg.data.decode()

    msg = "Received message subject:'{:s}' reply:'{:s}' payload:{:s}".format(subject, reply, payload)
    logger.info(msg)

    luigi.build([playlist.Playlist(youtube_url=payload), ])
