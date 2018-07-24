import logging
import asyncio

import luigi

from tasks import talks

logger = logging.getLogger(__name__)


@asyncio.coroutine
async def pipeline_talk_message_handler(msg):
    """ Talk message handler for the data pipeline scheduler.
    """
    subject = msg.subject
    reply = msg.reply
    payload = msg.data.decode()

    msg = "Received message subject:'{:s}' reply:'{:s}' payload:{:s}".format(subject, reply, payload)
    logger.info(msg)

    luigi.build([talks.wrappers.Complete(youtube_url=payload), ])
